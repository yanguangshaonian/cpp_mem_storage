
#include "lib.hpp"
#include "iostream"
using namespace std;

class Student {
    public:
        char name[3]{};
        uint32_t age;

        Student() = default;

        explicit Student(const uint32_t age)
            : age(age) {
            name[0] = '1';
        }
};

int main() {

    // auto mem_storage = MemoryStorage<Student, 5000>();
    // auto shared_store = mem_storage.build("student~");

    // shared_store->dangerous_access(0, [](Student& student, bool is_dangerous) {
    //     student.age += 1;
    //     cout << student.age << std::endl;
    // });


    auto mm = mem_storage::MemoryStorage<Student, 8>();
    mm.build("student~", 300000);
    auto& store = mm.get_view();

    store.try_locked_access(0, [](Student& student, bool is_dangerous) {
        student.age += 1;
        cout << student.age << std::endl;
    });

    std::cout << "Hello World!" << std::endl;
    return 0;
}