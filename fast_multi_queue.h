#pragma once

#include <future>
#include <vector>
#include <unordered_map>
#include <shared_mutex>
#include <queue>
#include <atomic>
#include <condition_variable>
#include <thread>

template<typename TKey, typename TValue>
struct IConsumer {
    virtual void Consume(const TKey& id, TValue&& value) = 0;
};

template<typename TKey, typename TValue>
class TFastMultiQueue {
public:
    TFastMultiQueue(size_t maxQueuesCount = std::numeric_limits<size_t>::max());

    virtual ~TFastMultiQueue();

    void Start(size_t workersCount = 1);
    void Stop();

    bool HasMessages();

    virtual void Subscribe(
            const TKey& id,
            IConsumer<TKey, TValue>* consumer,
            size_t max_queue_size = std::numeric_limits<size_t>::max(),
            size_t max_processing_jobs = 1);

    virtual void Unsubscribe(const TKey& id);

    virtual void Enqueue(const TKey& id, const TValue& value);
    virtual void Enqueue(const TKey& id, TValue&& value);

protected:
    struct TQueueRecord {
        TKey Id;

        IConsumer<TKey, TValue>* Consumer;
        std::atomic_bool IsActive;

        size_t MaxIncomingQueueSize;
        std::queue<TValue> IncomingQueue;

        size_t MaxProcessingJobs;
        std::atomic_size_t ProcessingJobs;
    };

    using TQueueRecordPtr = std::shared_ptr<TQueueRecord>;

protected:
    std::atomic_bool IsShutdown = false;

    size_t MaxQueuesCount;

    std::mutex IncomingLock;
    std::condition_variable IncomingCV;
    std::queue<TQueueRecordPtr> IncomingQueue;

    std::unordered_map<TKey, TQueueRecordPtr> RegisteredQueues;
    std::shared_mutex RegisteredQueuesLocker;

    std::vector<std::thread> Workers;

protected:
    void DoEnqueue();
};

#define FAST_MULTI_QUEUE_INL_H_
#include "fast_multi_queue_inl.h"
#undef FAST_MULTI_QUEUE_INL_H_
