#ifndef MEM_STORAGE_HPP
#define MEM_STORAGE_HPP
#include <iomanip>
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

    // 基础配置
    constexpr size_t CACHE_LINE_SIZE = 64;
    // 2MB 大页尺寸 (2 * 1024 * 1024)
    constexpr size_t HUGE_PAGE_SIZE = 2097152;
    // 初始映射读取头部的最小尺寸 (4KB 普通页即可)
    constexpr size_t MIN_MAP_SIZE = 4096;
    // 魔数
    constexpr uint64_t SHM_READY_MAGIC = 0xDEADBEEFCAFEBABE;

    // ----------------------------------------------------------------
    // 辅助工具: 向上对齐到 2MB
    // ----------------------------------------------------------------
    inline uint64_t align_to_huge_page(uint64_t size) {
        return (size + HUGE_PAGE_SIZE - 1) & ~(HUGE_PAGE_SIZE - 1);
    }

    // ----------------------------------------------------------------
    // 数据载体
    // ----------------------------------------------------------------
    template<class T>
    class alignas(CACHE_LINE_SIZE) PaddedValue {
        public:
            std::atomic<bool> busy_flag = false;
            T value;

            // 指数退避之后 强制抢占锁
            inline __attribute__((always_inline)) bool lock() noexcept {
                while (true) {
                    uint8_t delay = 0b0000'0001;
                    while (busy_flag.load(std::memory_order_relaxed)) {
                        for (auto i = 0; i < delay; i += 1) {
                            asm volatile("pause" ::: "memory");
                        }
                        // 之后强制 抢占锁
                        if (delay == 0b1000'0000) {
                            return false;
                        }
                        delay <<= 1;
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

    // ----------------------------------------------------------------
    // 元数据头
    // ----------------------------------------------------------------
    class alignas(CACHE_LINE_SIZE) ShmHeader {
        public:
            volatile uint64_t magic_num;
            uint64_t element_count;     // 逻辑元素数量
            uint64_t element_size;      // 单个元素大小
            uint64_t aligned_file_size; // 2MB 对齐后的文件总大小 (用于 mmap)
    };

    // ----------------------------------------------------------------
    // 视图 (View)
    // ----------------------------------------------------------------
    template<class T>
    class SharedDataView {
        private:
            PaddedValue<T>* data_ptr = nullptr;

        public:
            void init(uint8_t* base_addr) {
                data_ptr = reinterpret_cast<PaddedValue<T>*>(base_addr + sizeof(ShmHeader));
            }

            template<class Accesser>
            inline __attribute__((always_inline)) void dangerous_access(uint64_t idx, Accesser&& accesser) noexcept {
                auto& data_ref = data_ptr[idx];
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

    // ----------------------------------------------------------------
    // 存储管理器
    // ----------------------------------------------------------------
    template<class T>
    class MemoryStorage {
        private:
            enum class JoinResult {
                SUCCESS,
                FILE_NOT_FOUND, // 需要去创建
                DATA_CORRUPT,   // 文件存在但无效 (Magic 错误)
                TYPE_MISMATCH,  // 数据结构版本不一致
                SYSTEM_ERROR    // mmap 失败等系统级错误
            };

            string storage_name;
            SharedDataView<T> view;

            // 资源的信息, 析构时候用
            int32_t shm_fd = -1;
            uint8_t* mapped_ptr = nullptr;
            uint64_t mapped_size = 0;

            void log_msg(const string& level, const string& msg) {
                // 获取简短的时间戳
                auto now = system_clock::now();
                auto in_time_t = system_clock::to_time_t(now);
                std::tm bt{};
                localtime_r(&in_time_t, &bt);

                cout << "[" << std::put_time(&bt, "%T") << "] "
                     << "[" << level << "] "
                     << "[" << storage_name << "] " << msg << endl;
            }

            uint8_t* map_memory_segment(size_t size, bool use_hugepage) {
                int flags = MAP_SHARED;
                if (use_hugepage) {
                    flags |= MAP_HUGETLB;
                }
                auto ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, flags, this->shm_fd, 0);
                if (ptr == MAP_FAILED) {
                    return nullptr;
                }
                return reinterpret_cast<uint8_t*>(ptr);
            }

            // 尝试加入已存在的共享内存
            JoinResult try_join_existing() {
                this->shm_fd = shm_open(this->storage_name.c_str(), O_RDWR, 0660);
                if (this->shm_fd == -1) {
                    if (errno == ENOENT) {
                        return JoinResult::FILE_NOT_FOUND;
                    }
                    log_msg("ERROR", "shm_open 失败: " + string(strerror(errno)));
                    return JoinResult::SYSTEM_ERROR;
                }

                // 读取头部
                auto temp_ptr = map_memory_segment(MIN_MAP_SIZE, false);
                if (!temp_ptr) {
                    log_msg("ERROR", "读取metadata mmap 失败: " + string(strerror(errno)));
                    close(this->shm_fd);
                    return JoinResult::SYSTEM_ERROR;
                }

                auto header = reinterpret_cast<ShmHeader*>(temp_ptr);

                // 等待初始化 2s
                int wait_count = 0;
                while (header->magic_num != SHM_READY_MAGIC) {
                    wait_count += 1;
                    if (wait_count > 2000) {
                        log_msg("ERROR", "Magic 校验超时 (文件损坏或初始化挂起)");
                        munmap(temp_ptr, MIN_MAP_SIZE);
                        close(this->shm_fd);

                        // 尝试清理无效文件, 以便后续可以重新创建
                        shm_unlink(this->storage_name.c_str());
                        return JoinResult::DATA_CORRUPT;
                    }
                    std::this_thread::sleep_for(milliseconds(1));
                    std::atomic_thread_fence(std::memory_order_acquire);
                }

                // 读取关键元数据
                auto file_aligned_size = header->aligned_file_size;
                auto elem_cnt = header->element_count;
                auto elem_sz = header->element_size;

                if (elem_sz != sizeof(T)) {
                    log_msg("FATAL", "类型大小不匹配! 文件: " + to_string(elem_sz) + ", 本地: " + to_string(sizeof(T)));
                    munmap(temp_ptr, MIN_MAP_SIZE);
                    close(this->shm_fd);
                    return JoinResult::TYPE_MISMATCH;
                }

                // 解除临时映射, 然后完整映射
                munmap(temp_ptr, MIN_MAP_SIZE);
                this->mapped_ptr = map_memory_segment(file_aligned_size, true);

                // 容错降级: 如果 HugePage 失败, 回退到普通页
                if (this->mapped_ptr == nullptr) {
                    log_msg("WARN", "HugePage mmap 失败 (" + string(strerror(errno)) + "), 降级使用 4KB 页");
                    this->mapped_ptr = map_memory_segment(file_aligned_size, false);
                    if (this->mapped_ptr == nullptr) {
                        log_msg("ERROR", "降级 mmap 也失败: " + string(strerror(errno)));
                        close(this->shm_fd);
                        return JoinResult::SYSTEM_ERROR;
                    }
                }

                this->mapped_size = file_aligned_size;
                this->view.init(this->mapped_ptr);

                stringstream ss;
                ss << "复用成功。数量: " << elem_cnt << ", 占用空间: " << (file_aligned_size / 1024 / 1024) << " MB";
                log_msg("INFO", ss.str());

                return JoinResult::SUCCESS;
            }

            // 尝试创建新的共享内存
            bool try_create_new(uint64_t count) {
                // O_EXCL 保证原子性: 如果是我们创建的, 返回 fd; 如果已存在, 返回 error
                this->shm_fd = shm_open(this->storage_name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0660);
                if (this->shm_fd == -1) {
                    // 如果是因为文件存在失败, 返回 false 让上层重试 join
                    return false;
                }

                // 计算对齐后的大小
                auto raw_size = sizeof(ShmHeader) + sizeof(PaddedValue<T>) * count;
                auto aligned_sz = align_to_huge_page(raw_size);

                if (ftruncate(this->shm_fd, aligned_sz) == -1) {
                    log_msg("ERROR", "ftruncate 失败: " + string(strerror(errno)));
                    close(this->shm_fd);
                    shm_unlink(this->storage_name.c_str());
                    return false;
                }

                // 映射
                this->mapped_ptr = map_memory_segment(aligned_sz, true);
                if (this->mapped_ptr == nullptr) {
                    log_msg("WARN", "HugePage mmap 失败 (" + string(strerror(errno)) + "), 降级使用 4KB 页");
                    this->mapped_ptr = map_memory_segment(aligned_sz, false);
                    if (this->mapped_ptr == nullptr) {
                        log_msg("ERROR", "降级 mmap 也失败: " + string(strerror(errno)));
                        close(this->shm_fd);
                        shm_unlink(this->storage_name.c_str());
                        return false;
                    }
                }
                this->mapped_size = aligned_sz;

                // 初始化 Header
                auto header = new (this->mapped_ptr) ShmHeader();
                header->element_count = count;
                header->element_size = sizeof(T);
                header->aligned_file_size = aligned_sz;

                // 内存屏障, 确保数据写入后再设置 Magic
                std::atomic_thread_fence(std::memory_order_release);
                header->magic_num = SHM_READY_MAGIC;

                this->view.init(this->mapped_ptr);

                stringstream ss;
                ss << "创建成功。请求数量: " << count << ", 对齐后大小: " << (aligned_sz / 1024 / 1024) << " MB";
                log_msg("INFO", ss.str());

                return true;
            }

        public:
            // 返回 bool: true = 复用旧文件(Join), false = 创建新文件(Create)
            // 抛出异常: 严重错误
            bool build(string storage_name, uint64_t requested_count) {
                if (geteuid() != 0) {
                    throw std::runtime_error("致命错误: 需要 Root 权限以使用 HugePage/mmap");
                }
                this->storage_name = storage_name;

                for (auto i = 0; i < 3; i += 1) {
                    // 尝试 Join
                    auto join_res = try_join_existing();

                    if (join_res == JoinResult::SUCCESS) {
                        return true; // Is Join = true
                    }

                    if (join_res == JoinResult::TYPE_MISMATCH) {
                        throw std::runtime_error("共享内存数据结构版本不匹配");
                    }

                    if (join_res == JoinResult::DATA_CORRUPT) {
                        // 已在 try_join 中执行 unlink，直接进入下一次循环尝试 create
                        log_msg("WARN", "检测到损坏的文件，已删除并重试创建...");
                        continue;
                    }

                    // 尝试 Create
                    // join 返回 FILE_NOT_FOUND 或者其他可恢复错误时，尝试创建
                    if (try_create_new(requested_count)) {
                        return false; // Is Join = false (We are creator)
                    }

                    // 并发处理
                    if (errno == EEXIST) {
                        log_msg("WARN", "检测到并发竞争 (EEXIST)，正在重试 join...");
                        std::this_thread::sleep_for(std::chrono::milliseconds(10));
                        continue;
                    }

                    // 其他未知错误
                    throw std::runtime_error("shm_open 致命错误: " + string(strerror(errno)));
                }

                throw std::runtime_error("由于严重的并发竞争，初始化超时");
            }

            inline SharedDataView<T>& get_view() {
                return this->view;
            }

            ~MemoryStorage() {
                if (this->mapped_ptr) {
                    munmap(this->mapped_ptr, this->mapped_size);
                    log_msg("INFO", "内存映射已解除");
                }

                if (shm_fd != -1) {
                    close(shm_fd);
                }
            }
    };
}; // namespace mem_storage

#pragma pack(pop)
#endif //MEM_STORAGE_HPP
