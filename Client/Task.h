#ifndef CLEARMOON_RPC_TASK
#define CLEARMOON_RPC_TASK

#include <concepts>
#include <coroutine>
#include <exception>
#include <thread>
#include <utility>
#include <variant>

// 前向声明
template<typename T = void>
class Task;

// ============================================================================
// promise_type 基类（共享 unhandled_exception）
// ============================================================================
namespace detail
{
template<typename T, typename ResultVariant>
struct TaskPromiseBase
{
    ResultVariant             result;
    std::coroutine_handle<>   continuation_;

    std::suspend_always initial_suspend() noexcept { return {}; }

    struct final_awaiter
    {
        bool await_ready() const noexcept { return false; }
        template<typename Promise>
        void await_suspend(std::coroutine_handle<Promise> h) noexcept
        {
            if (h.promise().continuation_)
                h.promise().continuation_.resume();
        }
        void await_resume() noexcept {}
    };

    final_awaiter final_suspend() noexcept { return {}; }

    void unhandled_exception()
    {
        result.template emplace<std::exception_ptr>(std::current_exception());
    }
};

} // namespace detail

// ============================================================================
// Task<T> —— 非 void 版本
// ============================================================================
template<typename T>
class Task
{
public:
    struct promise_type : public detail::TaskPromiseBase<T, std::variant<std::monostate, T, std::exception_ptr>>
    {
        using Base = detail::TaskPromiseBase<T, std::variant<std::monostate, T, std::exception_ptr>>;

        Task get_return_object()
        {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        // co_return value — 仅 T ≠ void
        template<typename U>
            requires std::convertible_to<U, T>
        void return_value(U&& val)
        {
            this->result.template emplace<T>(std::forward<U>(val));
        }
    };

    using handle_t = std::coroutine_handle<promise_type>;
    explicit Task(handle_t h) : handle_(h) {}

    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, nullptr)) {}
    Task& operator=(Task&& other) noexcept
    {
        if (this != &other)
        {
            if (handle_) handle_.destroy();
            handle_ = std::exchange(other.handle_, nullptr);
        }
        return *this;
    }

    ~Task() { if (handle_) handle_.destroy(); }

    Task(const Task&)            = delete;
    Task& operator=(const Task&) = delete;

    // Awaitable 接口
    bool await_ready() const noexcept { return handle_.done(); }

    template<typename U>
    void await_suspend(std::coroutine_handle<U> caller)
    {
        handle_.promise().continuation_ = caller;
        handle_.resume();
    }

    T await_resume()
    {
        auto& result = handle_.promise().result;
        if (std::holds_alternative<std::exception_ptr>(result))
            std::rethrow_exception(std::get<std::exception_ptr>(result));
        return std::move(std::get<T>(result));
    }

    T get()
    {
        if (!handle_.done()) handle_.resume();
        while (!handle_.done())
            std::this_thread::yield();
        return await_resume();
    }

private:
    handle_t handle_;
};

// ============================================================================
// Task<void> 偏特化
// ============================================================================
template<>
class Task<void>
{
public:
    struct promise_type : public detail::TaskPromiseBase<void, std::variant<std::monostate, std::exception_ptr>>
    {
        using Base = detail::TaskPromiseBase<void, std::variant<std::monostate, std::exception_ptr>>;

        Task<void> get_return_object()
        {
            return Task<void>{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        // co_return; — 仅 T = void
        void return_void() {}
    };

    using handle_t = std::coroutine_handle<promise_type>;
    explicit Task(handle_t h) : handle_(h) {}

    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, nullptr)) {}
    Task& operator=(Task&& other) noexcept
    {
        if (this != &other)
        {
            if (handle_) handle_.destroy();
            handle_ = std::exchange(other.handle_, nullptr);
        }
        return *this;
    }

    ~Task() { if (handle_) handle_.destroy(); }

    Task(const Task&)            = delete;
    Task& operator=(const Task&) = delete;

    // Awaitable 接口
    bool await_ready() const noexcept { return handle_.done(); }

    template<typename U>
    void await_suspend(std::coroutine_handle<U> caller)
    {
        handle_.promise().continuation_ = caller;
        handle_.resume();
    }

    void await_resume()
    {
        auto& result = handle_.promise().result;
        if (std::holds_alternative<std::exception_ptr>(result))
            std::rethrow_exception(std::get<std::exception_ptr>(result));
    }

    void get()
    {
        if (!handle_.done()) handle_.resume();
        while (!handle_.done())
            std::this_thread::yield();
        await_resume();
    }

private:
    handle_t handle_;
};

#endif // CLEARMOON_RPC_TASK