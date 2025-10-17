#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <filesystem>
#include <iomanip>
#include <regex>

namespace fs = std::filesystem;

struct SubscriberResult {
    std::string subscriber_id;
    uint64_t messages_received;
    uint64_t duration_us;
    double throughput_msg_per_sec;
};

// Simple JSON value extractor
std::string extractJsonValue(const std::string& json, const std::string& key) {
    std::regex pattern("\"" + key + "\"\\s*:\\s*([^,}]+)");
    std::smatch match;
    if (std::regex_search(json, match, pattern)) {
        std::string value = match[1];
        // Remove quotes if present
        if (value.front() == '"') value = value.substr(1);
        if (value.back() == '"') value = value.substr(0, value.length() - 1);
        return value;
    }
    return "";
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: aggregator <results_directory> [<broker_type>]" << std::endl;
        std::cerr << "Example: aggregator /app/results redis" << std::endl;
        return 1;
    }

    std::string resultsDir = argv[1];
    std::string brokerType = argc > 2 ? argv[2] : "unknown";

    // Check if directory exists
    if (!fs::exists(resultsDir)) {
        std::cerr << "❌ Results directory not found: " << resultsDir << std::endl;
        return 1;
    }

    std::vector<SubscriberResult> results;

    // Read all JSON files from the results directory
    std::cout << "📂 Reading results from: " << resultsDir << std::endl;
    for (const auto& entry : fs::directory_iterator(resultsDir)) {
        if (entry.is_regular_file() && entry.path().extension() == ".json") {
            std::ifstream file(entry.path());
            if (file.is_open()) {
                try {
                    std::string json((std::istreambuf_iterator<char>(file)),
                                    std::istreambuf_iterator<char>());

                    SubscriberResult result;
                    result.subscriber_id = extractJsonValue(json, "subscriber_id");
                    result.messages_received = std::stoull(extractJsonValue(json, "messages_received"));
                    result.duration_us = std::stoull(extractJsonValue(json, "duration_us"));
                    result.throughput_msg_per_sec = std::stod(extractJsonValue(json, "throughput_msg_per_sec"));

                    if (!result.subscriber_id.empty() && result.messages_received > 0) {
                        results.push_back(result);
                        std::cout << "  ✓ " << entry.path().filename() << std::endl;
                    }
                } catch (const std::exception& e) {
                    std::cerr << "  ⚠️  Error parsing " << entry.path().filename() << ": " << e.what() << std::endl;
                }
                file.close();
            }
        }
    }

    if (results.empty()) {
        std::cerr << "❌ No results found in " << resultsDir << std::endl;
        return 1;
    }

    // Calculate aggregated statistics
    uint64_t total_messages = 0;
    uint64_t total_duration_us = 0;
    double total_throughput = 0;

    for (const auto& result : results) {
        total_messages += result.messages_received;
        total_duration_us += result.duration_us;
        total_throughput += result.throughput_msg_per_sec;
    }

    uint64_t avg_messages = total_messages / results.size();
    double avg_duration_us = static_cast<double>(total_duration_us) / results.size();
    double avg_duration_s = avg_duration_us / 1000000.0;
    double avg_throughput = total_throughput / results.size();
    double total_throughput_combined = total_messages / (total_duration_us / 1000000.0 / results.size());

    // Print results
    std::cout << "\n╔═══════════════════════════════════════════════╗" << std::endl;
    std::string title = brokerType + " Benchmark Results";
    std::cout << "║  " << std::setw(43) << std::left << title << "║" << std::endl;
    std::cout << "╚═══════════════════════════════════════════════╝" << std::endl;

    std::cout << "\n📊 Aggregated Results:" << std::endl;
    std::cout << "───────────────────────────────────────────────" << std::endl;
    std::cout << "  Subscriber Instances:   " << results.size() << std::endl;
    std::cout << "  Avg Messages/Instance:  " << avg_messages << std::endl;
    std::cout << "  Avg Duration:           " << std::fixed << std::setprecision(3) << avg_duration_s << " seconds" << std::endl;
    std::cout << "  Avg Throughput:         " << std::fixed << std::setprecision(2) << avg_throughput << " msg/sec" << std::endl;
    std::cout << "  Combined Throughput:    " << std::fixed << std::setprecision(2) << total_throughput_combined << " msg/sec" << std::endl;

    std::cout << "\n📋 Per-Instance Details:" << std::endl;
    std::cout << "───────────────────────────────────────────────" << std::endl;
    for (const auto& result : results) {
        std::cout << "  " << std::setw(25) << std::left << result.subscriber_id
                  << ": " << std::setw(12) << result.messages_received << " msgs, "
                  << std::fixed << std::setprecision(2) << result.throughput_msg_per_sec << " msg/sec" << std::endl;
    }

    std::cout << "\n";
    return 0;
}
