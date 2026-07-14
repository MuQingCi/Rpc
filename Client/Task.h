#ifndef CLEARMOON_RPC_TASK
#define CLEARMOON_RPC_TASK


#include <concepts>
#include <coroutine>
#include <exception>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>
template<typename T = void>
class Task
{
public:
    struct promise_type
    {
        std::variant<std::monostate, T, std::exception_ptr> result;
        std::coroutine_handle<> continuation_;

        Task get_return_object(){
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)}; }
        
        //协程初始化以及结束时的行为
        //always: 直接挂起; nerver: 不挂起
        std::suspend_always initial_suspend() noexcept { return{}; }

        struct final_awaiter{
            bool await_ready() const noexcept { return false; }
            void await_suspend(std::coroutine_handle<promise_type> h) noexcept
            {
                if(h.promise().continuation_)
                {
                    h.promise().continuation_.resume();
                }
            }
            void await_resume() noexcept {}
        };

        final_awaiter final_suspend() noexcept { return{}; }

        //co_return value
        template<typename U>
            requires std::convertible_to<U, T>
        void return_value(U&& val){
            result.template emplace<T>(std::forward<U>(val));
        }

        //co_return void
        void return_void() requires std::is_void_v<T>{}

        void unhandled_exception(){
            result.template emplace<std::exception_ptr>(std::current_exception());
        }
    };

    using handle_t = std::coroutine_handle<promise_type>;
    explicit Task(handle_t h) : handle_(h){}

    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, nullptr)){}

    Task& operator=(Task&& other) noexcept
    {
        if(this != other)
        {
            if(handle_) handle_.destroy();
            handle_ = std::exchange(other.handle_, nullptr);
        }
        return *this;
    }

    ~Task(){
        if(handle_) handle_.destroy();
    }

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    //Awaitable接口
    bool await_ready() const noexcept { return handle_.done(); }

    template<typename  U>
    void await_suspend(std::coroutine_handle<U> caller) { 
        handle_.promise().continuation_ = caller; 
        handle_.resume();
    }


    T await_resume()
    {
        auto& result = handle_.promise().result_;
        if(std::holds_alternative<std::exception_ptr>(result)){
            std::rethrow_exception(std::get<std::exception_ptr>(result));
        }
        if constexpr (!std::is_void_v<T>) {
            return std::move(std::get<T>(result));
        }
    }

    T get(){
        if(!handle_.done()) handle_.resume();
        while (!handle_.done()) {
            std::this_thread::yield();
        }
        return await_resume();
    }

private:
    handle_t handle_;
};


#endif