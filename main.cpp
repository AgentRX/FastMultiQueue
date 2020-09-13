#include "fast_multi_queue.h"

#include <iostream>
#include <random>
#include <map>
#include <cassert>

using namespace std;

const size_t GenMessageCount = 10000;

map<string, vector<string>> MultiMessages = {
    { "alpha", vector<string>(GenMessageCount) },
    { "bravo", vector<string>(GenMessageCount) },
    { "charlie", vector<string>(GenMessageCount) }
};

string GenRandomString() {
     string str("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");

     random_device rd;
     mt19937 generator(rd());

     shuffle(str.begin(), str.end(), generator);

     return str.substr(0, 32);
}

template <typename TKey, typename TValue>
struct TTestConsumer : public IConsumer<TKey, TValue> {
    void Consume(const TKey& /*id*/, TValue&& value) override {
        TValue LocalValue(std::move(value));

        // do some work
        this_thread::sleep_for(1ms);

        TotalProceeded++;
    }

    atomic_size_t TotalProceeded = 0;
};

struct TSimpleValue {
    int someUsefullInt = 0;

    TSimpleValue() = default;

    TSimpleValue(const TSimpleValue&) {
        // don't need to use it
        assert(false);
    }

    TSimpleValue(const TSimpleValue&& other) {
        someUsefullInt = other.someUsefullInt;
    }
};


static void PushThread(TFastMultiQueue<string, string>& queue, const string& key, const vector<string>& messages) {
    for (const auto& msg : messages) {
        while (true) {
            try {
                queue.Enqueue(key, msg);
                break;

            } catch (...) {
                // wait 1ms to free the queue
                this_thread::sleep_for(1ms);
            }
        }
    }
}

void TestQueueBaseBehaviour() {
    TFastMultiQueue<string, TSimpleValue> queue(1);
    TTestConsumer<string, TSimpleValue> consumer;

    cout << "Test queue default behaviour..." << endl;

    try {
        queue.Enqueue(MultiMessages.begin()->first, TSimpleValue());
        assert(false);
    } catch (invalid_argument&) { }

    try {
        queue.Subscribe(MultiMessages.begin()->first, nullptr, 1, 1);
        assert(false);
    } catch (invalid_argument&) { }

    queue.Subscribe(MultiMessages.begin()->first, &consumer, 1, 1);
    assert(!queue.HasMessages());

    try {
        queue.Subscribe(MultiMessages.begin()->first, &consumer, 1, 1);
        assert(false);
    } catch (invalid_argument&) { }

    try {
        queue.Subscribe("second id", &consumer, 1, 1);
        assert(false);
    } catch (length_error&) { }

    try {
        queue.Enqueue("second id", TSimpleValue());
        assert(false);
    } catch (invalid_argument&) { }

    queue.Enqueue(MultiMessages.begin()->first, TSimpleValue());
    assert(queue.HasMessages());

    try {
        queue.Enqueue(MultiMessages.begin()->first, TSimpleValue());
        assert(false);
    } catch (length_error&) { }

    queue.Start();
    // wait queue
    while (queue.HasMessages()) {
        this_thread::sleep_for(200ms);
    }
    assert(!queue.HasMessages());

    queue.Stop();
    queue.Enqueue(MultiMessages.begin()->first, TSimpleValue());
    assert(queue.HasMessages());

    queue.Unsubscribe(MultiMessages.begin()->first);
    assert(queue.HasMessages());

    queue.Start();
    // wait queue
    while (queue.HasMessages()) {
        this_thread::sleep_for(200ms);
    }
    assert(!queue.HasMessages());

    cout << "Done!" << endl << endl;
}

void TestQueue(size_t keyCount,
               size_t pushThreadsByQueue,
               size_t workThreads,
               size_t maxQueueSize,
               size_t keyJobsCount) {
    TFastMultiQueue<string, string> queue;
    TTestConsumer<string, string> consumer;

    cout << "Test " << keyCount << " queues, "
         << pushThreadsByQueue * keyCount * GenMessageCount << " messages, "
         << pushThreadsByQueue * keyCount << " pushing thread, "
         << workThreads << " work threads, "
         << keyJobsCount << " parallel jobs by queue..." << endl;

    auto MessagesIt = MultiMessages.begin();
    for (size_t idx = 0; idx < keyCount; idx++, MessagesIt++) {
        queue.Subscribe(MessagesIt->first, &consumer, maxQueueSize, keyJobsCount);
    }

    chrono::steady_clock::time_point start = chrono::steady_clock::now();
    queue.Start(workThreads);

    vector<thread> pushers;
    MessagesIt = MultiMessages.begin();
    for (size_t idx = 0; idx < keyCount; idx++, MessagesIt++) {
        for (size_t pos = 0; pos < pushThreadsByQueue; pos++) {
            pushers.emplace_back(thread(PushThread, ref(queue), MessagesIt->first, MessagesIt->second));
        }
    }

    // wait pushers
    for (auto& th: pushers) {
        if (th.joinable()) {
            th.join();
        }
    }
    chrono::steady_clock::time_point mid = chrono::steady_clock::now();
    cout << "Total pushing time: " << chrono::duration_cast<chrono::milliseconds>(mid - start).count() << " milliseconds" << endl;

    // wait queue
    while (queue.HasMessages()) {
        this_thread::sleep_for(200ms);
    }

    queue.Stop();
    chrono::steady_clock::time_point end = chrono::steady_clock::now();

    cout << "Done! Processed: " << consumer.TotalProceeded << " messages. "
         << "Total time: " << chrono::duration_cast<chrono::milliseconds>(end - start).count() << " milliseconds"
         << endl << endl;
}

int main() {
    cout << "Testing queue!" << endl;
    cout << "Generate testing messages..." << flush;

    for (const auto& key : MultiMessages) {
        for (size_t pos = 0; pos < GenMessageCount; pos++) {
            MultiMessages[key.first][pos] = GenRandomString();
        }
    }
    cout << "Done" << endl;

    TestQueueBaseBehaviour();

    cout << "Test with max size_t messages queue size" << endl;

    // one queue test
    TestQueue(1, 1, 1, numeric_limits<size_t>::max(), 1);
    TestQueue(1, 2, 10, numeric_limits<size_t>::max(), 1);
    TestQueue(1, 1, 1, numeric_limits<size_t>::max(), 10);
    TestQueue(1, 2, 10, numeric_limits<size_t>::max(), 10);
    TestQueue(1, 2, 10, numeric_limits<size_t>::max(), 100);

    // all queue test
    TestQueue(MultiMessages.size(), 1, 1, numeric_limits<size_t>::max(), 1);
    TestQueue(MultiMessages.size(), 2, 10, numeric_limits<size_t>::max(), 1);
    TestQueue(MultiMessages.size(), 1, 1, numeric_limits<size_t>::max(), 10);
    TestQueue(MultiMessages.size(), 2, 10, numeric_limits<size_t>::max(), 10);
    TestQueue(MultiMessages.size(), 2, 10, numeric_limits<size_t>::max(), 100);


    cout << "Test with 100 messages queue size" << endl;

    TestQueue(1, 2, 10, 100, 1);
    TestQueue(1, 1, 1, 100, 10);
    TestQueue(1, 2, 10, 100, 10);
    TestQueue(1, 2, 10, 100, 100);

    return 0;
}
