#ifndef MESSAGE_BROKER_H
#define MESSAGE_BROKER_H

#include <string>
#include <functional>

class MessageBroker {
public:
    virtual ~MessageBroker() = default;
    
    // Connection methods
    virtual bool connect() = 0;
    virtual void disconnect() = 0;
    virtual bool isConnected() const = 0;
    
    // Publisher methods
    virtual bool publish(const std::string& channel, const std::string& message) = 0;
    virtual void flush() = 0;
    
    // Subscriber methods
    virtual bool subscribe(const std::string& channel, 
                          std::function<void(const std::string&)> callback) = 0;
    virtual void unsubscribe(const std::string& channel) = 0;
    virtual void processMessages(int timeoutMs = 1000) = 0;
    
    // Utility
    virtual std::string getName() const = 0;
};

#endif // MESSAGE_BROKER_H
