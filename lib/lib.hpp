#ifndef MEM_STORAGE_HPP
#define MEM_STORAGE_HPP
#pragma pack(push)
#pragma pack()

#include <chrono>
#include <string>
#include <sys/mman.h>
#include <unistd.h>
#include <iostream>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <immintrin.h>
#include <fcntl.h>
#include <thread>
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <climits>
#include <cstdint>

namespace mem_storage {
    using namespace std;
    using namespace std::chrono;

    constexpr size_t CACHE_LINE_SIZE = 64;

    template<class T>
    class alignas(CACHE_LINE_SIZE) PaddedValue {
        public:
            std::atomic<bool> busy_flag = false;
            T value;

            // 指数退避之后 强制抢占锁
            inline __attribute__((always_inline)) bool lock() noexcept {
                while (true) {
                    uint8_t delay = 1;
                    while (busy_flag.load(std::memory_order_relaxed)) {
                        delay <<= 1;
                        // 之后强制 抢占锁
                        if (delay == 0xFF) {
                            return false;
                        }
                        for (auto i = 0; i < delay; ++i) {
                            asm volatile("pause" ::: "memory");
                        }
                    }

                    if (!busy_flag.exchange(true, std::memory_order_acquire)) {
                        return true;
                    }
                }
            }

            inline __attribute__((always_inline)) void unlock() noexcept {
                busy_flag.store(false, std::memory_order_release);
            }
    };

    template<class T, uint64_t CNT>
    class SharedDataStore {
        protected:
            alignas(CACHE_LINE_SIZE) PaddedValue<T> data[CNT];

        public:
            template<class Accesser>
            inline __attribute__((always_inline)) void dangerous_access(uint64_t idx, Accesser&& accesser) noexcept {
                auto& data_ref = data[idx];
                using ValueType = decltype(data_ref.value);
                if constexpr (std::is_invocable_v<Accesser, ValueType&, bool>) {
                    // 没抢到锁, 但是返回了, 说明 是有一些危险的
                    bool is_dangerous = (data_ref.lock() == false);
                    accesser(data_ref.value, is_dangerous);
                    data_ref.unlock();
                } else {
                    data_ref.lock();
                    accesser(data_ref.value);
                    data_ref.unlock();
                }
            }
    };

    // 内部有一个不安全的互斥锁, 保证读写互斥, 但是 他不是持续互斥的, 会在一定时间, 抢占锁, 强制释放
    template<class T, uint64_t CNT>
    class MemoryStorage {
        protected:
            using StoreType = SharedDataStore<T, CNT>;

            struct ShmLayout {
                public:
                    alignas(64) std::atomic<uint64_t> ready_flag{0};
                    alignas(64) StoreType store;
            };

            const uint64_t layout_size = sizeof(ShmLayout);
            ShmLayout* layout_ptr;

            string storage_name;
            int32_t shm_fd = -1;
            const uint64_t SHM_READY_MAGIC = 0xDEADBEEFCAFEBABE;

            void* map_memory_segment() {
                auto ptr =
                    mmap(nullptr, layout_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_HUGETLB, this->shm_fd, 0);
                if (ptr == MAP_FAILED) {
                    cout << "mmap 大页分配错误: " << strerror(errno) << ", 尝试回退" << endl;
                    ptr = mmap(nullptr, layout_size, PROT_READ | PROT_WRITE, MAP_SHARED, this->shm_fd, 0);
                }
                return ptr;
            }

            int try_join_existing() {
                this->shm_fd = shm_open(this->storage_name.c_str(), O_RDWR, 0660);
                if (this->shm_fd == -1) {
                    return -1; // 文件不存在，去创建
                }

                void* ptr = map_memory_segment();
                if (ptr == MAP_FAILED) {
                    cerr << "mmap 失败: " << strerror(errno) << endl;
                    close(this->shm_fd);
                    return -1;
                }

                auto* temp_layout = static_cast<ShmLayout*>(ptr);

                // 等待初始化完成(极短窗口)
                auto start = steady_clock::now();
                while (temp_layout->ready_flag.load(memory_order_acquire) != SHM_READY_MAGIC) {
                    if (steady_clock::now() - start > milliseconds(100)) {
                        cerr << ">> [警告] 检测到残留的损坏文件 (Magic无效), 正在清理..." << endl;
                        munmap(ptr, layout_size);
                        close(this->shm_fd);
                        shm_unlink(this->storage_name.c_str());
                        return -2; // 文件存在但无效(已执行unlink)
                    }
                    this_thread::sleep_for(milliseconds(1));
                }

                cout << ">> [复用成功] " << this->storage_name << " attached." << endl;
                this->layout_ptr = temp_layout;
                return 0;
            }

            bool try_create_new() {
                // O_EXCL 保证原子性：如果文件已存在则报错 EEXIST
                this->shm_fd = shm_open(this->storage_name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0660);
                if (this->shm_fd == -1) {
                    return false;
                }

                if (ftruncate(this->shm_fd, layout_size) == -1) {
                    cerr << "ftruncate 失败: " << strerror(errno) << endl;
                    close(this->shm_fd);
                    shm_unlink(this->storage_name.c_str());
                    return false;
                }

                void* ptr = map_memory_segment();
                if (ptr == MAP_FAILED) {
                    cerr << "mmap 失败: " << strerror(errno) << endl;
                    close(this->shm_fd);
                    shm_unlink(this->storage_name.c_str());
                    return false;
                }

                this->layout_ptr = static_cast<ShmLayout*>(ptr);
                // 初始化
                new (this->layout_ptr) ShmLayout();
                this->layout_ptr->ready_flag.store(SHM_READY_MAGIC, memory_order_release);

                cout << ">> [新建成功] " << this->storage_name << " created." << endl;
                return true;
            }

        public:
            bool build(string storage_name) {
                auto is_join = false;
                this->storage_name = storage_name;

                if (geteuid() != 0) {
                    throw std::runtime_error("Critical Error: 需要 root 权限以使用 HugePage/mmap");
                }

                // 重试循环：处理并发启动时的竞争
                int retries = 3;
                while (retries != 0) {
                    retries -= 1;

                    // 复用
                    int join_ret = try_join_existing();
                    if (join_ret == 0) {
                        is_join = true;
                        return is_join;
                    }

                    // 新建
                    if (try_create_new()) {
                        return is_join;
                    }

                    // 有些异常
                    if (errno == EEXIST) {
                        cout << ">> [并发竞争] 检测到其他进程刚刚创建了文件，重试..." << endl;
                        std::this_thread::sleep_for(std::chrono::milliseconds(10));
                        continue;
                    }

                    // 其他错误
                    throw std::runtime_error("shm_open 致命错误 (" + to_string(errno) + "): " + strerror(errno));
                }
                throw std::runtime_error("初始化超时: " + storage_name + " 存在严重的并发启动竞争");
            }

            inline __attribute__((always_inline)) StoreType& get_store() const {
                return this->layout_ptr->store;
            }

            ~MemoryStorage() {
                if (this->layout_ptr) {
                    munmap(this->layout_ptr, layout_size);
                    cout << this->storage_name << " munmap 已完成" << endl;
                }

                if (shm_fd != -1) {
                    close(shm_fd);
                    cout << this->storage_name << " close fd 已完成" << endl;
                }
            }
    };

}; // namespace mem_storage

#pragma pack(pop)
#endif //MEM_STORAGE_HPP
