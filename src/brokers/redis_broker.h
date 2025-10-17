#ifndef REDIS_BROKER_H
#define REDIS_BROKER_H

#include "message_broker.h"
#include <hiredis/hiredis.h>
#include <cstring>
#include <map>

class RedisBroker : public MessageBroker {
private:
    redisContext* ctx = nullptr;
    redisContext* subCtx = nullptr;
    std::string host;
    int port;
    std::map<std::string, std::function<void(const std::string&)>> callbacks;
    int pipelineCount = 0;
    const int BATCH_SIZE = 100;

public:
    RedisBroker(const std::string& h = "localhost", int p = 6379)
        : host(h), port(p) {}
    
    ~RedisBroker() override {
        disconnect();
    }
    
    bool connect() override {
        ctx = redisConnect(host.c_str(), port);
        if (ctx == nullptr || ctx->err) {
            return false;
        }
        return true;
    }
    
    void disconnect() override {
        if (ctx != nullptr) {
            redisFree(ctx);
            ctx = nullptr;
        }
        if (subCtx != nullptr) {
            redisFree(subCtx);
            subCtx = nullptr;
        }
    }
    
    bool isConnected() const override {
        return ctx != nullptr && ctx->err == 0;
    }
    
    bool publish(const std::string& channel, const std::string& message) override {
        if (!isConnected()) return false;
        
        // Use redisCommand for immediate send (slower but ensures delivery for pub/sub)
        redisReply* reply = (redisReply*)redisCommand(ctx, "PUBLISH %s %s", channel.c_str(), message.c_str());
        bool success = (reply != nullptr);
        if (reply != nullptr) {
            freeReplyObject(reply);
        }
        return success;
    }
    
    void flush() override {
        if (!isConnected() || pipelineCount == 0) return;
        
        // Read all pending replies - this triggers sending buffered commands
        for (int i = 0; i < pipelineCount; i++) {
            redisReply* reply = nullptr;
            if (redisGetReply(ctx, (void**)&reply) == REDIS_OK && reply != nullptr) {
                freeReplyObject(reply);
            }
        }
        pipelineCount = 0;
    }
    
    bool subscribe(const std::string& channel,
                  std::function<void(const std::string&)> callback) override {
        if (subCtx == nullptr) {
            subCtx = redisConnect(host.c_str(), port);
            if (subCtx == nullptr || subCtx->err) {
                return false;
            }
        }
        
        callbacks[channel] = callback;
        
        // Send SUBSCRIBE command (async)
        if (redisAppendCommand(subCtx, "SUBSCRIBE %s", channel.c_str()) != REDIS_OK) {
            return false;
        }
        
        // Read subscription confirmation immediately
        redisReply* reply = nullptr;
        if (redisGetReply(subCtx, (void**)&reply) != REDIS_OK || reply == nullptr) {
            return false;
        }
        
        // Verify subscription was successful
        bool success = (reply->type == REDIS_REPLY_ARRAY && reply->elements >= 3 &&
                       strcmp(reply->element[0]->str, "subscribe") == 0);
        freeReplyObject(reply);
        
        return success;
    }
    
    void unsubscribe(const std::string& channel) override {
        if (subCtx != nullptr) {
            redisReply* reply = (redisReply*)redisCommand(subCtx, "UNSUBSCRIBE %s", channel.c_str());
            if (reply != nullptr) {
                freeReplyObject(reply);
            }
        }
        callbacks.erase(channel);
    }
    
    void processMessages(int timeoutMs = 1000) override {
        if (subCtx == nullptr) return;
        
        struct timeval timeout;
        timeout.tv_sec = timeoutMs / 1000;
        timeout.tv_usec = (timeoutMs % 1000) * 1000;
        redisSetTimeout(subCtx, timeout);
        
        redisReply* reply = nullptr;
        if (redisGetReply(subCtx, (void**)&reply) == REDIS_OK && reply != nullptr) {
            if (reply->type == REDIS_REPLY_ARRAY && reply->elements >= 3) {
                if (strcmp(reply->element[0]->str, "message") == 0) {
                    std::string channel = reply->element[1]->str;
                    std::string message = reply->element[2]->str;
                    
                    auto it = callbacks.find(channel);
                    if (it != callbacks.end()) {
                        it->second(message);
                    }
                }
            }
            freeReplyObject(reply);
        }
    }
    
    std::string getName() const override {
        return "Redis";
    }
};

#endif // REDIS_BROKER_H
