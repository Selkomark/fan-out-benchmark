#include "benchmark_common.h"
#include "config.h"
#include "message_broker.h"
#include "redis_broker.h"
#include "nats_broker.h"
#include <chrono>
#include <iomanip>
#include <thread>
#include <vector>
#include <atomic>
#include <memory>

std::atomic<uint64_t> totalMessagesPublished(0);
std::mutex resultsMutex;

std::unique_ptr<MessageBroker> createBroker(const std::string& brokerType) {
    if (brokerType == "redis") {
        return std::make_unique<RedisBroker>(
            std::getenv("REDIS_HOST") ? std::getenv("REDIS_HOST") : "localhost",
            std::getenv("REDIS_PORT") ? std::atoi(std::getenv("REDIS_PORT")) : 6379
        );
    } else if (brokerType == "nats") {
        return std::make_unique<NatsBroker>(
            std::getenv("NATS_URL") ? std::getenv("NATS_URL") : "nats://localhost:4222"
        );
    }
    return nullptr;
}

void publisherThread(int publisherId,
                    int numPublishers,
                    int publishDurationSeconds,
                    MessageBroker* broker,
                    const std::string& channel,
                    std::chrono::steady_clock::time_point endTime,
                    std::chrono::steady_clock::time_point& firstMessageTime,
                    std::chrono::steady_clock::time_point& lastMessageTime) {
    uint64_t messagesPublished = 0;
    uint64_t messageCounter = 0;

    // Send START marker only from first publisher
    if (publisherId == 0) {
        broker->publish(channel, "START_BENCHMARK");
        broker->flush();  // Flush immediately to ensure START signal is sent
        
        {
            std::lock_guard<std::mutex> lock(resultsMutex);
            firstMessageTime = std::chrono::steady_clock::now();
        }
    }

    // Publish messages for the specified duration
    while (std::chrono::steady_clock::now() < endTime) {
        std::string message = "msg_" + std::to_string(publisherId) + "_" + std::to_string(messageCounter++);
        if (broker->publish(channel, message)) {
            messagesPublished++;
        }
    }
    
    // Flush any pending messages
    broker->flush();

    // Send END marker only from first publisher
    if (publisherId == 0) {
        broker->publish(channel, "END_BENCHMARK");
        broker->flush();
        
        {
            std::lock_guard<std::mutex> lock(resultsMutex);
            lastMessageTime = std::chrono::steady_clock::now();
        }
    }

    totalMessagesPublished.fetch_add(messagesPublished, std::memory_order_relaxed);
}

int main() {
    // Load configuration from .env file
    Config config;
    int numPublishers = config.getInt("NUM_PUBLISHERS", 10);
    int publishDurationSeconds = config.getInt("PUBLISH_DURATION_SECONDS", 60);
    
    config.print();

    // Determine broker type from first argument or environment variable
    std::string brokerType = "redis"; // default
    if (std::getenv("BROKER_TYPE")) {
        brokerType = std::getenv("BROKER_TYPE");
    }
    
    auto broker = createBroker(brokerType);
    if (!broker) {
        std::cerr << "❌ Unknown broker type: " << brokerType << std::endl;
        return 1;
    }

    printHeader(broker->getName() + " Publisher Benchmark");
    
    std::cout << "\n🚀 Starting " << broker->getName() << " Publisher..." << std::endl;

    // Connect to broker
    if (!broker->connect()) {
        std::cerr << "❌ Connection error" << std::endl;
        return 1;
    }
    std::cout << "✓ Connected to " << broker->getName() << std::endl;

    // Calculate end time
    auto startTime = std::chrono::steady_clock::now();
    auto endTime = startTime + std::chrono::seconds(publishDurationSeconds);

    std::cout << "   Starting " << numPublishers << " concurrent publishers for " 
              << publishDurationSeconds << " seconds..." << std::endl;

    std::chrono::steady_clock::time_point firstMessageTime;
    std::chrono::steady_clock::time_point lastMessageTime;

    // Launch publisher threads
    std::vector<std::thread> threads;
    for (int i = 0; i < numPublishers; i++) {
        threads.emplace_back(publisherThread, i, numPublishers, publishDurationSeconds,
                           broker.get(), "benchmark_channel",
                           endTime, std::ref(firstMessageTime), std::ref(lastMessageTime));
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    auto overallEndTime = std::chrono::steady_clock::now();

    // Calculate and display results
    auto totalDuration = std::chrono::duration_cast<std::chrono::milliseconds>(overallEndTime - startTime);
    double totalSeconds = totalDuration.count() / 1000.0;
    double throughput = totalSeconds > 0 ? totalMessagesPublished / totalSeconds : 0;

    std::cout << "\n========================================" << std::endl;
    std::cout << broker->getName() << " Publisher Results:" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Concurrent Publishers:  " << numPublishers << std::endl;
    std::cout << "Duration:               " << publishDurationSeconds << " seconds" << std::endl;
    std::cout << "Messages Published:     " << totalMessagesPublished << std::endl;
    std::cout << "Total Duration:         " << std::fixed << std::setprecision(3) << totalSeconds << " seconds" << std::endl;
    std::cout << "Publish Throughput:     " << std::fixed << std::setprecision(0) << throughput << " msg/sec" << std::endl;
    std::cout << "Avg per Publisher:      " << std::fixed << std::setprecision(0) 
              << (throughput / numPublishers) << " msg/sec" << std::endl;
    std::cout << "========================================\n" << std::endl;

    std::cout << "✅ " << broker->getName() << " Publisher Complete!\n" << std::endl;

    broker->disconnect();
    return 0;
}
