#ifndef BENCHMARK_COMMON_H
#define BENCHMARK_COMMON_H

#include <iostream>
#include <atomic>
#include <chrono>
#include <iomanip>
#include <mutex>
#include <condition_variable>


// Thread-safe message counter
class MessageCounter {
public:
    void increment() {
        counter.fetch_add(1, std::memory_order_relaxed);
    }
    
    uint64_t get() const {
        return counter.load(std::memory_order_relaxed);
    }
    
    void reset() {
        counter.store(0, std::memory_order_relaxed);
    }

private:
    std::atomic<uint64_t> counter{0};
};

// Synchronization barrier for coordinating multiple threads
class Barrier {
public:
    explicit Barrier(int count) : threshold(count), count(count), generation(0) {}
    
    void wait() {
        std::unique_lock<std::mutex> lock(mtx);
        auto gen = generation;
        if (--count == 0) {
            generation++;
            count = threshold;
            cv.notify_all();
        } else {
            cv.wait(lock, [this, gen] { return gen != generation; });
        }
    }

private:
    std::mutex mtx;
    std::condition_variable cv;
    int threshold;
    int count;
    int generation;
};

// Print benchmark results in a formatted way
inline void printResults(const std::string& name, 
                         const std::chrono::steady_clock::time_point& startTime,
                         const std::chrono::steady_clock::time_point& endTime,
                         uint64_t messagesReceived,
                         int numSubscribers,
                         int numMessages) {
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
    double seconds = duration.count() / 1000.0;
    uint64_t expectedMessages = static_cast<uint64_t>(numMessages) * numSubscribers;
    double throughputMsg = numMessages / seconds;
    double throughputTotal = messagesReceived / seconds;
    double deliveryRate = (messagesReceived * 100.0) / expectedMessages;
    
    std::cout << "\n========================================" << std::endl;
    std::cout << name << " Results:" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Messages Published:    " << numMessages << std::endl;
    std::cout << "Number of Subscribers: " << numSubscribers << std::endl;
    std::cout << "Expected Deliveries:   " << expectedMessages << std::endl;
    std::cout << "Messages Received:     " << messagesReceived << std::endl;
    std::cout << "Delivery Rate:         " << std::fixed << std::setprecision(2) 
              << deliveryRate << "%" << std::endl;
    std::cout << "Duration:              " << std::fixed << std::setprecision(3) 
              << seconds << " seconds" << std::endl;
    std::cout << "Publish Throughput:    " << std::fixed << std::setprecision(0) 
              << throughputMsg << " msg/sec" << std::endl;
    std::cout << "Total Throughput:      " << std::fixed << std::setprecision(0) 
              << throughputTotal << " deliveries/sec" << std::endl;
    std::cout << "========================================\n" << std::endl;
}

// Print a formatted header
inline void printHeader(const std::string& title) {
    std::cout << "\n╔═══════════════════════════════════════════════╗" << std::endl;
    std::cout << "║   " << std::left << std::setw(43) << title << "║" << std::endl;
    std::cout << "╚═══════════════════════════════════════════════╝" << std::endl;
}

// Print configuration
inline void printConfiguration(int numMessages, int numSubscribers = 3) {
    std::cout << "\nTest Configuration:" << std::endl;
    std::cout << "  - Messages to publish: " << numMessages << std::endl;
    std::cout << "  - Subscribers: " << numSubscribers << " (defined in docker-compose.yml)" << std::endl;
    std::cout << "  - Total expected deliveries: " << (numMessages * numSubscribers) << std::endl;
}

#endif // BENCHMARK_COMMON_H

