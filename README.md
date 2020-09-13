# FastMultiQueue
Fast queue with several producers and one consumer for every queue. The queue is thread-safe with multithreading processing. Queue allows to forward messages via move semantics.

# How to build
To build tests use cmake:
```console
foo@bar:~$ cmake ./
foo@bar:~$ make
```

# How to run
```console
foo@bar:~$ ./FastMultiQueue
```

# Requirenments
* c++17 or above
* gcc 10 or above
* cmake 3.5 or above

# How to use
* Include 'fast_multi_queue.h'
* Create Consumer inherited from IConsumer
* Create TFastMultiQueue object with maxQueuesCount param
* Register a new queue via Subscribe method
* Produce new messages via Enqueue method

# Example:
```cpp
template <typename TKey, typename TValue>
struct TSimpleConsumer : public IConsumer<TKey, TValue> {
    void Consume(const TKey& /*id*/, TValue&& value) override {
        // do some work
    }
};

TFastMultiQueue<string, string> queue(10);
TSimpleConsumer<string, string> consumer;

queue.Subscribe('key', &consumer, 10, 10);
queue.Subscribe('other', &consumer, 10, 10);

// start processing
queue.Start(10);

// post messages
queue.Enqueue('key', 'message');
queue.Enqueue('other', 'message');

// stop
queue.Stop();

// wait queue end
while (queue.HasMessages()) {
    this_thread::sleep_for(200ms);
}

```