#include "benchmark_common.h"
#include "message_broker.h"
#include "redis_broker.h"
#include "nats_broker.h"
#include <chrono>
#include <iomanip>
#include <atomic>
#include <memory>
#include <cstring>
#include <fstream> // Added for file writing
#include <cstdlib> // Added for system calls
#include <unistd.h>
#include <sstream>
#include <sys/stat.h>

std::atomic<uint64_t> messagesReceived(0);
std::atomic<bool> benchmarkStarted(false);
std::atomic<bool> benchmarkEnded_Flag(false);
std::chrono::steady_clock::time_point startTime;
std::chrono::steady_clock::time_point endTime;

// Global broker type for results writing
std::string g_brokerType;

// Forward declaration
void writeResults(const char* subscriberId);

std::unique_ptr<MessageBroker> createBroker(const std::string& brokerType) {
    if (brokerType == "redis") {
        return std::make_unique<RedisBroker>(
            std::getenv("REDIS_HOST") ? std::getenv("REDIS_HOST") : "localhost",
            std::getenv("REDIS_PORT") ? std::getenv("REDIS_PORT") ? std::atoi(std::getenv("REDIS_PORT")) : 6379 : 6379
        );
    } else if (brokerType == "nats") {
        return std::make_unique<NatsBroker>(
            std::getenv("NATS_URL") ? std::getenv("NATS_URL") : "nats://localhost:4222"
        );
    }
    return nullptr;
}

void messageCallback(const std::string& message) {
    if (message == "START_BENCHMARK") {
        benchmarkStarted = true;
        startTime = std::chrono::steady_clock::now();
    } else if (message == "END_BENCHMARK") {
        if (!benchmarkEnded_Flag) {
            endTime = std::chrono::steady_clock::now();
            benchmarkEnded_Flag = true;
        }
    } else if (benchmarkStarted && !benchmarkEnded_Flag) {
        messagesReceived++;
    }
}

int main() {
    const char* subscriberId = std::getenv("SUBSCRIBER_ID") ? std::getenv("SUBSCRIBER_ID") : "subscriber_1";
    
    // Determine broker type from environment variable
    g_brokerType = "redis";
    if (std::getenv("BROKER_TYPE")) {
        g_brokerType = std::getenv("BROKER_TYPE");
    }
    std::string brokerType = g_brokerType;
    
    auto broker = createBroker(brokerType);
    if (!broker) {
        std::cerr << "❌ Unknown broker type: " << brokerType << std::endl;
        return 1;
    }

    // Connect to broker
    if (!broker->connect()) {
        std::cerr << "❌ Connection error to " << broker->getName() << std::endl;
        return 1;
    }
    std::cerr << "✓ Connected to " << broker->getName() << std::endl;

    // Subscribe to channel
    if (!broker->subscribe("benchmark_channel", messageCallback)) {
        std::cerr << "❌ Subscription error" << std::endl;
        return 1;
    }
    std::cerr << "✓ Subscribed to benchmark_channel" << std::endl;
    std::cerr << "✓ Subscriber ready - waiting for messages (will run until stopped)" << std::endl;

    // Run continuously - process messages and write results when benchmark ends
    bool resultsWritten = false;
    
    while (true) {
        broker->processMessages(100);
        
        // Write results once when benchmark ends, but keep running
        if (benchmarkEnded_Flag && !resultsWritten) {
            writeResults(subscriberId);
            resultsWritten = true;
            std::cerr << "✓ Benchmark results written - subscriber continues running" << std::endl;
        }
    }
    
    // This code is unreachable but kept for clarity
    broker->disconnect();
    return 0;
}

void writeResults(const char* subscriberId) {
    // Calculate and log results
    auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
    double seconds = duration_us.count() / 1000000.0;
    double throughput = seconds > 0 ? messagesReceived / seconds : 0;

    // Output results as JSON to stdout
    std::cout << "\n{\n";
    std::cout << "  \"subscriber_id\": \"" << subscriberId << "\",\n";
    std::cout << "  \"messages_received\": " << messagesReceived << ",\n";
    std::cout << "  \"duration_us\": " << duration_us.count() << ",\n";
    std::cout << "  \"duration_ms\": " << duration_ms.count() << ",\n";
    std::cout << "  \"throughput_msg_per_sec\": " << std::fixed << std::setprecision(2) << throughput << "\n";
    std::cout << "}\n" << std::endl;
    std::cout.flush();
    std::cerr.flush();

    // Also persist results to shared /data volume for post-run analytics (DuckDB)
    // Ensure /data exists
    struct stat st;
    if (stat("/data", &st) != 0) {
        ::mkdir("/data", 0755);
    }
    const char* hostnameEnv = std::getenv("HOSTNAME");
    char hostbuf[128] = {0};
    if (!hostnameEnv) {
        if (gethostname(hostbuf, sizeof(hostbuf) - 1) != 0) {
            std::snprintf(hostbuf, sizeof(hostbuf), "unknown-host");
        }
    }
    std::string hostname = hostnameEnv ? std::string(hostnameEnv) : std::string(hostbuf);

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

    std::ostringstream filepath;
    filepath << batchDir << "/" << g_brokerType << "_" << subscriberId << "_" << hostname << "_" << tsbuf << ".json";
    std::ofstream out(filepath.str());
    if (out.is_open()) {
        out << "{\n";
        out << "  \"batch_id\": \"" << batchId << "\",\n";
        out << "  \"broker_type\": \"" << g_brokerType << "\",\n";
        out << "  \"subscriber_id\": \"" << subscriberId << "\",\n";
        out << "  \"host\": \"" << hostname << "\",\n";
        out << "  \"timestamp\": \"" << tsbuf << "\",\n";
        out << "  \"messages_received\": " << messagesReceived << ",\n";
        out << "  \"duration_us\": " << duration_us.count() << ",\n";
        out << "  \"duration_ms\": " << duration_ms.count() << ",\n";
        out << "  \"throughput_msg_per_sec\": " << std::fixed << std::setprecision(2) << throughput << "\n";
        out << "}\n";
        out.flush();
        out.close();
        std::cerr << "✓ Wrote results to " << filepath.str() << std::endl;
    } else {
        std::cerr << "⚠️  Failed to write results file: " << filepath.str() << std::endl;
    }

    // Human-readable output
    std::cout << "\n========================================" << std::endl;
    std::cout << "Subscriber Results (" << subscriberId << "):" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Messages Received:      " << messagesReceived << std::endl;
    std::cout << "Duration:               " << std::fixed << std::setprecision(3) << seconds << " seconds" << std::endl;
    std::cout << "Throughput:             " << std::fixed << std::setprecision(2) << throughput << " msg/sec" << std::endl;
    std::cout << "========================================\n" << std::endl;
    std::cout.flush();
}
