#ifndef NATS_BROKER_H
#define NATS_BROKER_H

#include "message_broker.h"
#include <nats.h>
#include <map>

class NatsBroker : public MessageBroker {
private:
    natsConnection* conn = nullptr;
    std::string url;
    std::map<std::string, natsSubscription*> subscriptions;
    std::map<std::string, std::function<void(const std::string&)>> callbacks;

    static void messageHandler(natsConnection* nc, natsSubscription* sub,
                              natsMsg* msg, void* closure) {
        NatsBroker* broker = static_cast<NatsBroker*>(closure);
        std::string channel(natsMsg_GetSubject(msg));
        std::string message(natsMsg_GetData(msg), natsMsg_GetDataLength(msg));
        
        auto it = broker->callbacks.find(channel);
        if (it != broker->callbacks.end()) {
            it->second(message);
        }
        natsMsg_Destroy(msg);
    }

public:
    NatsBroker(const std::string& u = "nats://localhost:4222")
        : url(u) {}
    
    ~NatsBroker() override {
        disconnect();
    }
    
    bool connect() override {
        natsStatus status = natsConnection_ConnectTo(&conn, url.c_str());
        return status == NATS_OK;
    }
    
    void disconnect() override {
        for (auto& sub : subscriptions) {
            if (sub.second != nullptr) {
                natsSubscription_Destroy(sub.second);
            }
        }
        subscriptions.clear();
        callbacks.clear();
        
        if (conn != nullptr) {
            natsConnection_Close(conn);
            natsConnection_Destroy(conn);
            conn = nullptr;
        }
    }
    
    bool isConnected() const override {
        return conn != nullptr;
    }
    
    bool publish(const std::string& channel, const std::string& message) override {
        if (!isConnected()) return false;
        natsStatus status = natsConnection_Publish(conn, channel.c_str(),
                                                   message.c_str(), message.length());
        return status == NATS_OK;
    }
    
    void flush() override {
        if (isConnected()) {
            natsConnection_Flush(conn);
        }
    }
    
    bool subscribe(const std::string& channel,
                  std::function<void(const std::string&)> callback) override {
        if (!isConnected()) return false;
        
        natsSubscription* sub = nullptr;
        natsStatus status = natsConnection_Subscribe(&sub, conn, channel.c_str(),
                                                     messageHandler, this);
        
        if (status != NATS_OK) {
            return false;
        }
        
        subscriptions[channel] = sub;
        callbacks[channel] = callback;
        return true;
    }
    
    void unsubscribe(const std::string& channel) override {
        auto it = subscriptions.find(channel);
        if (it != subscriptions.end()) {
            natsSubscription_Unsubscribe(it->second);
            natsSubscription_Destroy(it->second);
            subscriptions.erase(it);
        }
        callbacks.erase(channel);
    }
    
    void processMessages(int timeoutMs = 1000) override {
        if (!isConnected()) return;
        // NATS handles message delivery asynchronously via callbacks
        // Just sleep briefly to allow message processing
        nats_Sleep(timeoutMs);
    }
    
    std::string getName() const override {
        return "NATS";
    }
};

#endif // NATS_BROKER_H
