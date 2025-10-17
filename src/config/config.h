#ifndef CONFIG_H
#define CONFIG_H

#include <fstream>
#include <sstream>
#include <string>
#include <map>
#include <iostream>

class Config {
private:
    std::map<std::string, std::string> values;

    static std::string trim(const std::string& str) {
        size_t start = str.find_first_not_of(" \t\r\n");
        size_t end = str.find_last_not_of(" \t\r\n");
        if (start == std::string::npos) return "";
        return str.substr(start, end - start + 1);
    }

public:
    Config() {
        loadFromEnv();
    }

    explicit Config(const std::string& filepath) {
        loadFromFile(filepath);
    }

    void loadFromFile(const std::string& filepath) {
        std::ifstream file(filepath);
        if (!file.is_open()) {
            std::cerr << "Warning: Could not open config file: " << filepath << std::endl;
            return;
        }

        std::string line;
        while (std::getline(file, line)) {
            // Skip empty lines and comments
            if (line.empty() || line[0] == '#') continue;

            // Find the = sign
            size_t pos = line.find('=');
            if (pos == std::string::npos) continue;

            std::string key = trim(line.substr(0, pos));
            std::string value = trim(line.substr(pos + 1));

            if (!key.empty() && !value.empty()) {
                values[key] = value;
            }
        }

        file.close();
    }

    void loadFromEnv() {
        // Try to load from .env file in current directory
        loadFromFile(".env");
    }

    std::string get(const std::string& key, const std::string& defaultValue = "") const {
        auto it = values.find(key);
        if (it != values.end()) {
            return it->second;
        }
        return defaultValue;
    }

    int getInt(const std::string& key, int defaultValue = 0) const {
        auto it = values.find(key);
        if (it != values.end()) {
            try {
                return std::stoi(it->second);
            } catch (...) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    bool has(const std::string& key) const {
        return values.find(key) != values.end();
    }

    void print() const {
        std::cout << "\n📋 Configuration Loaded:" << std::endl;
        for (const auto& pair : values) {
            std::cout << "  " << pair.first << " = " << pair.second << std::endl;
        }
        std::cout << std::endl;
    }
};

#endif // CONFIG_H
