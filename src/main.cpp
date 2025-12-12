
#include "lib.hpp"

class Student {
    public:
        char name[66]{};
        uint32_t age;

        Student() = default;

        explicit Student(const uint32_t age)
            : age(age) {
            name[0] = '1';
            name[65] = '\n';
        }
};

int main() {

    auto mem_storage = MemoryStorage<Student, 5000>();
    auto shared_store = mem_storage.build("student~");

    shared_store->dangerous_access(0, [](Student& student, bool is_dangerous) {
        student.age += 1;
        cout << student.age << std::endl;
    });
    
    std::cout << "Hello World!" << std::endl;
    return 0;
}