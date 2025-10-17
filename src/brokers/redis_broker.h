#ifndef REDIS_BROKER_H
#define REDIS_BROKER_H

#include "message_broker.h"
#include <hiredis/hiredis.h>
#include <cstring>
#include <map>
#include <chrono>
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

class RedisBroker : public MessageBroker {
private:
    redisContext* ctx = nullptr;
    redisContext* subCtx = nullptr;
    std::string host;
    int port;
    std::map<std::string, std::function<void(const std::string&)>> callbacks;
    int pipelineCount = 0;
    const int BATCH_SIZE = 1000;  // Balanced for throughput and responsiveness
    bool timeoutConfigured = false;  // Per-instance timeout tracking

public:
    RedisBroker(const std::string& h = "localhost", int p = 6379)
        : host(h), port(p) {}
    
    ~RedisBroker() override {
        disconnect();
    }
    
    bool connect() override {
        struct timeval timeout = { 5, 0 };  // 5 second timeout for pub context
        ctx = redisConnectWithTimeout(host.c_str(), port, timeout);
        if (ctx == nullptr || ctx->err) {
            return false;
        }
        
        // Set read/write timeouts to prevent indefinite blocking
        redisSetTimeout(ctx, timeout);
        
        // CRITICAL: Enable TCP_NODELAY to disable Nagle's algorithm
        // This prevents batching/delaying of small packets, essential for low-latency pub/sub
        int fd = ctx->fd;
        int yes = 1;
        if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) < 0) {
            std::cerr << "Warning: Failed to set TCP_NODELAY on Redis connection" << std::endl;
        }
        
        // Increase socket buffer sizes for better throughput
        int sndbuf = 1024 * 1024;  // 1MB send buffer
        int rcvbuf = 1024 * 1024;  // 1MB receive buffer
        setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
        setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
        
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
        
        // Use synchronous publish with TCP_NODELAY for reliable delivery
        // TCP_NODELAY ensures low latency despite synchronous calls
        redisReply* reply = (redisReply*)redisCommand(ctx, "PUBLISH %s %b", 
                                                       channel.c_str(), 
                                                       message.c_str(), 
                                                       message.length());
        bool success = (reply != nullptr);
        if (reply != nullptr) {
            freeReplyObject(reply);
        }
        return success;
    }
    
    void flush() override {
        if (!isConnected() || pipelineCount == 0) return;
        
        int count = pipelineCount;
        pipelineCount = 0;  // Reset immediately to avoid issues
        
        // Read all pending replies - this triggers sending buffered commands
        for (int i = 0; i < count; i++) {
            redisReply* reply = nullptr;
            int status = redisGetReply(ctx, (void**)&reply);
            
            if (status != REDIS_OK) {
                if (ctx->err) {
                    std::cerr << "Redis flush error: " << ctx->errstr << std::endl;
                }
                // Clean up any remaining replies
                while (i < count) {
                    redisReply* dummy = nullptr;
                    if (redisGetReply(ctx, (void**)&dummy) == REDIS_OK && dummy != nullptr) {
                        freeReplyObject(dummy);
                    }
                    i++;
                }
                return;
            }
            
            if (reply != nullptr) {
                freeReplyObject(reply);
            }
        }
    }
    
    bool subscribe(const std::string& channel,
                  std::function<void(const std::string&)> callback) override {
        if (subCtx == nullptr) {
            subCtx = redisConnect(host.c_str(), port);
            if (subCtx == nullptr || subCtx->err) {
                return false;
            }
            
            // CRITICAL: Enable TCP_NODELAY on subscription connection too
            int fd = subCtx->fd;
            int yes = 1;
            if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) < 0) {
                std::cerr << "Warning: Failed to set TCP_NODELAY on Redis subscription" << std::endl;
            }
            
            // Increase socket buffer sizes
            int sndbuf = 1024 * 1024;  // 1MB
            int rcvbuf = 1024 * 1024;  // 1MB
            setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
            setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
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
        
        // Configure timeout once per instance (not static!)
        if (!timeoutConfigured) {
            struct timeval timeout;
            timeout.tv_sec = 0;
            timeout.tv_usec = 1000;  // 1ms timeout for non-blocking behavior
            
            if (redisSetTimeout(subCtx, timeout) == REDIS_OK) {
                timeoutConfigured = true;
            }
        }
        
        // Process multiple messages per call for better throughput
        auto endTime = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        
        while (std::chrono::steady_clock::now() < endTime) {
            redisReply* reply = nullptr;
            int status = redisGetReply(subCtx, (void**)&reply);
            
            if (status == REDIS_OK && reply != nullptr) {
                // Successfully received a message
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
            } else if (status == REDIS_ERR) {
                // Check if it's just a timeout (EAGAIN/EWOULDBLOCK) which is normal
                if (subCtx->err == REDIS_ERR_IO) {
                    // Clear the error - timeout is not a fatal error for pub/sub
                    subCtx->err = 0;
                    memset(subCtx->errstr, 0, sizeof(subCtx->errstr));
                    break;  // No more messages available, exit loop
                } else if (subCtx->err != 0) {
                    // Actual error - log it
                    std::cerr << "Redis subscriber error: " << subCtx->errstr << std::endl;
                    break;
                }
            } else {
                // No reply available
                break;
            }
        }
    }
    
    std::string getName() const override {
        return "Redis";
    }
};

#endif // REDIS_BROKER_H
