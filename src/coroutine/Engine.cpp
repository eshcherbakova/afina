#include <afina/coroutine/Engine.h>
#include <csetjmp>
#include <iostream>
#include <stdio.h>
#include <string.h>

namespace Afina {
namespace Coroutine {
/**
 * Save stack of the current coroutine in the given context
 */
void Engine::Store(context &ctx) {

    char stack_addr;

    if (&stack_addr > ctx.Low) {
        ctx.Hight = const_cast<char *>(&stack_addr) + 1;
    } else {
        ctx.Low = const_cast<char *>(&stack_addr);
    }

    uint32_t &av_size = std::get<1>(ctx.Stack);
    auto size = ctx.Hight - ctx.Low;

    if (av_size < size) {
        delete[] std::get<0>(ctx.Stack);
        std::get<0>(ctx.Stack) = new char[size];
        std::get<1>(ctx.Stack) = size;
    }

    memcpy(std::get<0>(ctx.Stack), ctx.Low, size);
}

/**
 * Restore stack of the given context and pass control to coroutinne
 */
void Engine::Restore(context &ctx) {
    char stack_addr;
    if (&stack_addr >= ctx.Low && &stack_addr <= ctx.Hight) {
        Restore(ctx);
    }

    memcpy(ctx.Low, std::get<0>(ctx.Stack), ctx.Hight - ctx.Low);
    cur_routine = &ctx;
    longjmp(ctx.Environment, 1);
}

/**
 * Gives up current routine execution and let engine to schedule other one. It is not defined when
 * routine will get execution back, for example if there are no other coroutines then executing could
 * be trasferred back immediately (yield turns to be noop).
 *
 * Also there are no guarantee what coroutine will get execution, it could be caller of the current one or
 * any other which is ready to run
 */
void Engine::yield() {

    if (!alive || (cur_routine == alive && !alive->next)) {
        return;
    }
    context *candidate = alive;

    if (candidate == cur_routine) {
        candidate = candidate->next;
    }

    Enter(*candidate);
}

void Engine::Enter(context &ctx) {
    if (cur_routine && cur_routine != idle_ctx) {
        if (setjmp(cur_routine->Environment) > 0) {
            return;
        }
        Store(*cur_routine);
    }

    Restore(ctx);
}
/**
 * Suspend current routine and transfers control to the given one, resumes its execution from the point
 * when it has been suspended previously.
 *
 * If routine to pass execution to is not specified (nullptr) then method should behaves like yield. In case
 * if passed routine is the current one method does nothing
 */
void Engine::sched(void *routine_) {

    if (cur_routine == routine_) {
        return;
    }

    if (routine_) {
        Enter(*(static_cast<context *>(routine_)));
    } else {
        yield();
    }
}

/**
 * Blocks current routine so that is can't be scheduled anymore
 * If it was a currently running corountine, then do yield to select new one to be run instead.
 *
 * If argument is nullptr then block current coroutine
 */
void Engine::block(void *coro) {
    context *blockedCoro;

    if (!coro) {
        blockedCoro = cur_routine;
    } else {
        blockedCoro = static_cast<context *>(coro);
    }

    if (!blockedCoro || blockedCoro->is_blocked) {
        return;
    }
    blockedCoro->is_blocked = true;

    if (alive == blockedCoro) {
        alive = alive->next;
    }
    if (blockedCoro->prev) {
        blockedCoro->prev->next = blockedCoro->next;
    }
    if (blockedCoro->next) {
        blockedCoro->next->prev = blockedCoro->prev;
    }

    blockedCoro->prev = nullptr;
    blockedCoro->next = blocked;
    blocked = blockedCoro;
    if (blocked->next) {
        blocked->next->prev = blockedCoro;
    }
    if (blockedCoro == cur_routine) {
        Enter(*idle_ctx);
    }
}

/**
 * Put coroutine back to list of alive, so that it could be scheduled later
 */
void Engine::unblock(void *coro) {
    auto unblockedCoro = static_cast<context *>(coro);

    if (!unblockedCoro || !unblockedCoro->is_blocked) {
        return;
    }
    unblockedCoro->is_blocked = false;

    if (blocked == unblockedCoro) {
        blocked = blocked->next;
    }
    if (unblockedCoro->prev) {
        unblockedCoro->prev->next = unblockedCoro->next;
    }
    if (unblockedCoro->next) {
        unblockedCoro->next->prev = unblockedCoro->prev;
    }

    unblockedCoro->prev = nullptr;
    unblockedCoro->next = alive;
    alive = unblockedCoro;
    if (alive->next) {
        alive->next->prev = unblockedCoro;
    }
}

} // namespace Coroutine
} // namespace Afina
