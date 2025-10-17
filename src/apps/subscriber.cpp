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
        endTime = std::chrono::steady_clock::now();
        benchmarkEnded_Flag = true;
    } else if (benchmarkStarted) {
        messagesReceived++;
    }
}

int main() {
    const char* subscriberId = std::getenv("SUBSCRIBER_ID") ? std::getenv("SUBSCRIBER_ID") : "subscriber_1";
    
    // Determine broker type from environment variable
    std::string brokerType = "redis";
    if (std::getenv("BROKER_TYPE")) {
        brokerType = std::getenv("BROKER_TYPE");
    }
    
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

    // Wait for messages until benchmark ends or timeout
    bool benchmarkEnded = false;
    auto waitStart = std::chrono::steady_clock::now();
    
    // Get timeout from environment variable PUBLISH_DURATION_SECONDS + buffer
    int publishDuration = 10; // default
    if (std::getenv("PUBLISH_DURATION_SECONDS")) {
        publishDuration = std::atoi(std::getenv("PUBLISH_DURATION_SECONDS"));
    }
    const int TIMEOUT_SECONDS = publishDuration + 15; // Publish duration + 15 second buffer
    const int NO_MESSAGE_TIMEOUT_SECONDS = 15; // Exit if no START received after 15 seconds
    
    while (!benchmarkEnded) {
        broker->processMessages(100);
        
        auto elapsedOverall = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - waitStart);
        
        // Exit if benchmark ended
        if (benchmarkEnded_Flag) {
            benchmarkEnded = true;
        }
        // Exit if no START message received after NO_MESSAGE_TIMEOUT_SECONDS
        else if (elapsedOverall.count() > NO_MESSAGE_TIMEOUT_SECONDS && !benchmarkStarted) {
            benchmarkEnded = true;
            // Set times if benchmark never started (so we output valid results)
            if (startTime.time_since_epoch().count() == 0) {
                startTime = waitStart;
            }
            if (endTime.time_since_epoch().count() == 0) {
                endTime = std::chrono::steady_clock::now();
            }
        }
        // Exit if total timeout exceeded
        else if (elapsedOverall.count() > TIMEOUT_SECONDS) {
            benchmarkEnded = true;
            // Set times if benchmark never started (so we output valid results)
            if (startTime.time_since_epoch().count() == 0) {
                startTime = waitStart;
            }
            if (endTime.time_since_epoch().count() == 0) {
                endTime = std::chrono::steady_clock::now();
            }
        }
    }
    
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
    filepath << batchDir << "/" << brokerType << "_" << subscriberId << "_" << hostname << "_" << tsbuf << ".json";
    std::ofstream out(filepath.str());
    if (out.is_open()) {
        out << "{\n";
        out << "  \"batch_id\": \"" << batchId << "\",\n";
        out << "  \"broker_type\": \"" << brokerType << "\",\n";
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

    broker->disconnect();
    return 0;
}
