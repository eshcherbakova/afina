#include <afina/concurrency/Executor.h>
#include <iostream>

namespace Afina {
namespace Concurrency {

Executor::Executor(int low_watermark, int high_watermark, int max_queue_size, int idle_time) :
        low_watermark(low_watermark), high_watermark(high_watermark), max_queue_size(max_queue_size),
        idle_time(idle_time), state(Executor::State::kRun), threads_cnt(low_watermark), busy_workers_cnt(low_watermark) {
        for (int i = 0; i < low_watermark; i++) {
        auto new_tr = std::thread([this]() { perform(this); });
        new_tr.detach();
        }
}

Executor::~Executor() {}

void Executor::Stop(bool await) {
    std::unique_lock<std::mutex> l(mutex);
    if (threads_cnt != 0) {
        state = State::kStopping;
    } else {
        state = State::kStopped;
    }
    empty_condition.notify_one();
    while (await && threads_cnt) {
        stop_condition.wait(l);
    }
}

void perform(Executor *executor) {
    while (true) {
        std::unique_lock<std::mutex> l(executor->mutex);

        if (executor->tasks.empty()) {
            executor->busy_workers_cnt--;
        }

        while (true) {
            while (executor->tasks.empty() && executor->state == Executor::State::kRun) {
                auto until_time = std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(executor->idle_time);
                if (executor->empty_condition.wait_until(l, until_time) == std::cv_status::timeout) {
                 break;
                }
             }   

            if (!executor->tasks.empty()) {
                break;
            }

            if (executor->state != Executor::State::kRun) {
                executor->threads_cnt--;
                if (executor->threads_cnt == 0) {
                    executor->state = Executor::State::kStopped;
                    l.unlock();
                    executor->stop_condition.notify_all();
                }
                l.unlock();
                executor->empty_condition.notify_one();
                return;
            }
            
            if (executor->threads_cnt == executor->low_watermark) {
                continue;
            }
            
            executor->threads_cnt--;
            return;

        }

        auto task = executor->tasks.front();
        executor->tasks.pop_front();
        l.unlock();

        try {
                task();
            } catch (std::runtime_error& ex) {
                std::cout << "Error happened while performing task: " << ex.what() << std::endl;
            }
                      
    }
}
        
}
} // namespace Afina
