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
#include <fstream>
#include <sstream>
#include <cstring>
#include <unistd.h>
#include <sys/stat.h>

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
                    const std::string& brokerType,
                    const std::string& channel,
                    std::chrono::steady_clock::time_point endTime,
                    std::chrono::steady_clock::time_point& firstMessageTime,
                    std::chrono::steady_clock::time_point& lastMessageTime) {
    uint64_t messagesPublished = 0;
    uint64_t messageCounter = 0;

    // Each thread needs its own connection (Redis connections are NOT thread-safe!)
    auto broker = createBroker(brokerType);
    if (!broker) {
        std::cerr << "❌ Thread " << publisherId << " failed to create broker" << std::endl;
        return;
    }
    
    if (!broker->connect()) {
        std::cerr << "❌ Thread " << publisherId << " failed to connect" << std::endl;
        return;
    }
    
    std::cerr << "✓ Thread " << publisherId << " connected successfully" << std::endl;

    // Send START marker only from first publisher
    if (publisherId == 0) {
        broker->publish(channel, "START_BENCHMARK");
        broker->flush();  // Flush immediately to ensure START signal is sent
        
        // Give subscribers a moment to receive and process START signal before flood begins
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        
        {
            std::lock_guard<std::mutex> lock(resultsMutex);
            firstMessageTime = std::chrono::steady_clock::now();
        }
    }
    
    // Wait for all threads to receive START signal before publishing
    std::this_thread::sleep_for(std::chrono::milliseconds(250));

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
    
    std::cerr << "✓ Thread " << publisherId << " published " << messagesPublished << " messages" << std::endl;
    
    // Clean up thread-local connection
    broker->disconnect();
    
    std::cerr << "✓ Thread " << publisherId << " disconnected and exiting" << std::endl;
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
    
    // Create a test broker just to get the name (threads will create their own connections)
    auto testBroker = createBroker(brokerType);
    if (!testBroker) {
        std::cerr << "❌ Unknown broker type: " << brokerType << std::endl;
        return 1;
    }

    printHeader(testBroker->getName() + " Publisher Benchmark");
    
    std::cout << "\n🚀 Starting " << testBroker->getName() << " Publisher..." << std::endl;
    std::cout << "✓ Each publisher thread will create its own connection" << std::endl;

    // Calculate end time
    auto startTime = std::chrono::steady_clock::now();
    auto endTime = startTime + std::chrono::seconds(publishDurationSeconds);

    std::cout << "   Starting " << numPublishers << " concurrent publishers for " 
              << publishDurationSeconds << " seconds..." << std::endl;

    std::chrono::steady_clock::time_point firstMessageTime;
    std::chrono::steady_clock::time_point lastMessageTime;

    // Launch publisher threads - each will create its own connection
    std::vector<std::thread> threads;
    for (int i = 0; i < numPublishers; i++) {
        threads.emplace_back(publisherThread, i, numPublishers, publishDurationSeconds,
                           brokerType, "benchmark_channel",
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
    std::cout << testBroker->getName() << " Publisher Results:" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Concurrent Publishers:  " << numPublishers << std::endl;
    std::cout << "Duration:               " << publishDurationSeconds << " seconds" << std::endl;
    std::cout << "Messages Published:     " << totalMessagesPublished << std::endl;
    std::cout << "Total Duration:         " << std::fixed << std::setprecision(3) << totalSeconds << " seconds" << std::endl;
    std::cout << "Publish Throughput:     " << std::fixed << std::setprecision(0) << throughput << " msg/sec" << std::endl;
    std::cout << "Avg per Publisher:      " << std::fixed << std::setprecision(0) 
              << (throughput / numPublishers) << " msg/sec" << std::endl;
    std::cout << "========================================\n" << std::endl;

    // Write results to JSON file for analytics
    struct stat st;
    if (stat("/data", &st) != 0) {
        ::mkdir("/data", 0755);
    }
    
    // Get hostname
    const char* hostnameEnv = std::getenv("HOSTNAME");
    char hostbuf[128] = {0};
    if (!hostnameEnv) {
        if (gethostname(hostbuf, sizeof(hostbuf) - 1) != 0) {
            std::snprintf(hostbuf, sizeof(hostbuf), "unknown-host");
        }
    }
    std::string hostname = hostnameEnv ? std::string(hostnameEnv) : std::string(hostbuf);

    // Get timestamp
    auto now = std::chrono::system_clock::now();
    std::time_t now_c = std::chrono::system_clock::to_time_t(now);
    std::tm tm_struct;
#ifdef _WIN32
    localtime_s(&tm_struct, &now_c);
#else
    localtime_r(&now_c, &tm_struct);
#endif
    char tsbuf[32];
    std::strftime(tsbuf, sizeof(tsbuf), "%Y%m%dT%H%M%S", &tm_struct);

    // Determine batch id and directory
    const char* batchEnv = std::getenv("BATCH_ID");
    std::string batchId = batchEnv ? std::string(batchEnv) : std::string(tsbuf);
    std::string batchDir = std::string("/data/") + batchId;
    if (stat(batchDir.c_str(), &st) != 0) {
        ::mkdir(batchDir.c_str(), 0755);
    }

    // Get environment configuration
    int numSubscribers = 1;
    if (std::getenv("NUM_SUBSCRIBERS")) {
        numSubscribers = std::atoi(std::getenv("NUM_SUBSCRIBERS"));
    }

    std::ostringstream filepath;
    filepath << batchDir << "/" << brokerType << "_publisher_" << hostname << "_" << tsbuf << ".json";
    std::ofstream out(filepath.str());
    if (out.is_open()) {
        out << "{\n";
        out << "  \"batch_id\": \"" << batchId << "\",\n";
        out << "  \"broker_type\": \"" << brokerType << "\",\n";
        out << "  \"role\": \"publisher\",\n";
        out << "  \"host\": \"" << hostname << "\",\n";
        out << "  \"timestamp\": \"" << tsbuf << "\",\n";
        out << "  \"config\": {\n";
        out << "    \"num_publishers\": " << numPublishers << ",\n";
        out << "    \"num_subscribers\": " << numSubscribers << ",\n";
        out << "    \"publish_duration_seconds\": " << publishDurationSeconds << "\n";
        out << "  },\n";
        out << "  \"results\": {\n";
        out << "    \"messages_published\": " << totalMessagesPublished << ",\n";
        out << "    \"duration_ms\": " << totalDuration.count() << ",\n";
        out << "    \"duration_seconds\": " << std::fixed << std::setprecision(3) << totalSeconds << ",\n";
        out << "    \"throughput_msg_per_sec\": " << std::fixed << std::setprecision(2) << throughput << ",\n";
        out << "    \"avg_per_publisher_msg_per_sec\": " << std::fixed << std::setprecision(2) << (throughput / numPublishers) << "\n";
        out << "  }\n";
        out << "}\n";
        out.flush();
        out.close();
        std::cerr << "✓ Wrote publisher results to " << filepath.str() << std::endl;
    } else {
        std::cerr << "⚠️  Failed to write publisher results file: " << filepath.str() << std::endl;
    }

    std::cout << "✅ " << testBroker->getName() << " Publisher Complete!\n" << std::endl;

    return 0;
}
