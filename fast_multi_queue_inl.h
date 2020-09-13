#pragma once

#ifndef FAST_MULTI_QUEUE_INL_H_
#error "Direct inclusion of this file is not allowed, use fast_multi_queue.h"
#endif

#include <functional>
#include <chrono>
#include <iostream>

// hack for ide
#include "fast_multi_queue.h"

using namespace std;
using namespace std::chrono_literals;

template<typename TKey, typename TValue>
TFastMultiQueue<TKey, TValue>::TFastMultiQueue(size_t maxQueuesCount)
    : MaxQueuesCount(maxQueuesCount)
{
}

template<typename TKey, typename TValue>
TFastMultiQueue<TKey, TValue>::~TFastMultiQueue() {
    Stop();
}

template<typename TKey, typename TValue>
void TFastMultiQueue<TKey, TValue>::Start(size_t workersCount) {
    IsShutdown = false;
    for (size_t pos = 0; pos < workersCount; pos++) {
        Workers.emplace_back(thread(&TFastMultiQueue<TKey, TValue>::DoEnqueue, this));
    }
}

template<typename TKey, typename TValue>
void TFastMultiQueue<TKey, TValue>::Stop() {
    IsShutdown = true;

    for (auto& th: Workers) {
        if (th.joinable()) {
            th.join();
        }
    }
}

template<typename TKey, typename TValue>
bool TFastMultiQueue<TKey, TValue>::HasMessages() {
    unique_lock<mutex> locker(IncomingLock);
    return !IncomingQueue.empty();
}

template<typename TKey, typename TValue>
void TFastMultiQueue<TKey, TValue>::Subscribe(const TKey& id,
    IConsumer<TKey, TValue>* consumer,
    size_t max_queue_size,
    size_t max_processing_jobs) {

    unique_lock writeLocker(RegisteredQueuesLocker);

    // check exist
    if (consumer == nullptr) {
        throw invalid_argument("Consumer must not be null!");
    }

    // check exist
    if (RegisteredQueues.find(id) != RegisteredQueues.end()) {
        throw invalid_argument("The queue is already exists!");
    }

    // check limit
    if (RegisteredQueues.size() >= MaxQueuesCount) {
        throw length_error("Maximum queue limit exceed!");
    }

    auto rec = make_shared<TQueueRecord>();
    rec->Id = id;
    rec->Consumer = consumer;
    rec->IsActive = true;
    rec->MaxIncomingQueueSize = max_queue_size;
    rec->MaxProcessingJobs = max_processing_jobs;
    rec->ProcessingJobs = 0;

    RegisteredQueues.emplace(make_pair(id, std::move(rec)));
}

template<typename TKey, typename TValue>
void TFastMultiQueue<TKey, TValue>::Unsubscribe(const TKey& id) {
    unique_lock writeLocker(RegisteredQueuesLocker);

    auto recIt = RegisteredQueues.find(id);
    if (recIt != RegisteredQueues.end()) {
        recIt->second->IsActive = false;

        // wait until all current jobs done
        while (recIt->second->ProcessingJobs > 0) {
            this_thread::sleep_for(100ms);
        }

        RegisteredQueues.erase(recIt);
    }
}

template<typename TKey, typename TValue>
void TFastMultiQueue<TKey, TValue>::Enqueue(const TKey& id, const TValue& value) {
    TValue valueCopy(value);
    Enqueue(id, move(valueCopy));
}

template<typename TKey, typename TValue>
void TFastMultiQueue<TKey, TValue>::Enqueue(const TKey& id, TValue&& value) {
    TQueueRecordPtr rec;

    {
        shared_lock readLocker(RegisteredQueuesLocker);

        auto recIt = RegisteredQueues.find(id);
        if (recIt == RegisteredQueues.end()) {
            throw invalid_argument("Can't find queue for specified key!");
        }

        rec = recIt->second;
    }

    {
        unique_lock<mutex> locker(IncomingLock);
        // check limits
        if (rec->IncomingQueue.size() >= rec->MaxIncomingQueueSize) {
            throw length_error("Maximum queue limit exceed!");
        }

        // enqueue
        rec->IncomingQueue.emplace(move(value));

        // create new task
        IncomingQueue.emplace(rec);
    }
    // notify
    IncomingCV.notify_one();
}

template<typename TKey, typename TValue>
void TFastMultiQueue<TKey, TValue>::DoEnqueue() {
    while (!IsShutdown) {
        unique_lock<mutex> locker(IncomingLock);

        // check incoming queue
        if (!IncomingQueue.empty() || IncomingCV.wait_for(locker, 100ms) != cv_status::timeout) {
            if (IncomingQueue.empty()) {
                continue;
            }
            // get task
            auto task = move(IncomingQueue.front());
            IncomingQueue.pop();

            if (task->IsActive) {
                // take job slot
                if (++task->ProcessingJobs > task->MaxProcessingJobs || task->IncomingQueue.empty()) {
                    task->ProcessingJobs--;

                    // reschedule task
                    IncomingQueue.emplace(move(task));
                    continue;

                }
                // get value to process
                TValue value = move(task->IncomingQueue.front());
                task->IncomingQueue.pop();

                // lock is not nessesary more
                locker.unlock();

                // process value
                task->Consumer->Consume(task->Id, move(value));

                // free job slot
                task->ProcessingJobs--;
            }
        }
    }
}
