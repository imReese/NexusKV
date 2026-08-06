#ifndef NEXUSKV_CLIENT_H
#define NEXUSKV_CLIENT_H

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace nexuskv {

enum class ReuseDisposition { FULL_HIT, PARTIAL_HIT, MISS, BYPASS };

enum class ClientHealthStatus { HEALTHY, DEGRADED, FAIL_OPEN_FALLBACK };

struct RoutingDecision {
    std::string worker_id;
    std::string worker_address;
    int matched_token_count;
    double estimated_compute_saving_ms;
    ReuseDisposition disposition;
};

struct PinnedBlockHandle {
    uint64_t block_id;
    uint64_t physical_addr;
    size_t size_bytes;
    int device_id;
    bool is_valid;
};

struct ClientConfig {
    std::string control_plane_addr = "127.0.0.1:9098";
    bool enable_fail_open = true;
    double timeout_ms = 50.0;
    std::string preferred_backend = "CUDA_IPC";
};

struct ClientMetrics {
    uint64_t total_routing_requests;
    uint64_t total_cache_hits;
    uint64_t total_evicted_handles;
};

class NexusKVClient {
   public:
    explicit NexusKVClient(ClientConfig config = ClientConfig())
        : config_(std::move(config)),
          health_status_(ClientHealthStatus::HEALTHY),
          total_routing_requests_(0),
          total_cache_hits_(0),
          total_evicted_handles_(0) {}

    explicit NexusKVClient(std::string control_plane_addr)
        : health_status_(ClientHealthStatus::HEALTHY),
          total_routing_requests_(0),
          total_cache_hits_(0),
          total_evicted_handles_(0) {
        config_.control_plane_addr = std::move(control_plane_addr);
    }

    ClientHealthStatus GetHealthStatus() const { return health_status_; }

    void ConfigureTransport(const std::string& preferred_backend, double timeout_ms) {
        std::lock_guard<std::mutex> lock(mutex_);
        config_.preferred_backend = preferred_backend;
        config_.timeout_ms = timeout_ms;
    }

    RoutingDecision RoutePrefix(const std::vector<int>& prompt_tokens,
                                const std::vector<std::string>& candidate_workers) {
        total_routing_requests_.fetch_add(1, std::memory_order_relaxed);

        RoutingDecision decision;
        decision.matched_token_count = static_cast<int>(prompt_tokens.size() * 0.8);
        if (!candidate_workers.empty()) {
            decision.worker_id = candidate_workers[0];
            decision.worker_address = candidate_workers[0];
        } else {
            decision.worker_id = "worker-0";
            decision.worker_address = "127.0.0.1:8080";
        }
        decision.estimated_compute_saving_ms = decision.matched_token_count * 0.05;

        if (decision.matched_token_count > 0) {
            total_cache_hits_.fetch_add(1, std::memory_order_relaxed);
            decision.disposition = (decision.matched_token_count == prompt_tokens.size())
                                       ? ReuseDisposition::FULL_HIT
                                       : ReuseDisposition::PARTIAL_HIT;
        } else {
            decision.disposition = ReuseDisposition::MISS;
        }

        return decision;
    }

    bool HandshakePrefillDecode(const std::string& request_id, const std::string& prefill_node,
                                const std::string& decode_node,
                                const std::vector<PinnedBlockHandle>& blocks) {
        if (request_id.empty() || blocks.empty()) {
            return false;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& block : blocks) {
            if (block.is_valid && block.block_id > 0) {
                active_handles_[block.block_id] = block;
            }
        }
        return true;
    }

    bool EvictHandle(uint64_t block_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = active_handles_.find(block_id);
        if (it != active_handles_.end()) {
            active_handles_.erase(it);
            total_evicted_handles_.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
        return false;
    }

    ClientMetrics GetMetrics() const {
        ClientMetrics m;
        m.total_routing_requests = total_routing_requests_.load(std::memory_order_relaxed);
        m.total_cache_hits = total_cache_hits_.load(std::memory_order_relaxed);
        m.total_evicted_handles = total_evicted_handles_.load(std::memory_order_relaxed);
        return m;
    }

   private:
    ClientConfig config_;
    ClientHealthStatus health_status_;
    std::mutex mutex_;
    std::unordered_map<uint64_t, PinnedBlockHandle> active_handles_;
    std::atomic<uint64_t> total_routing_requests_;
    std::atomic<uint64_t> total_cache_hits_;
    std::atomic<uint64_t> total_evicted_handles_;
};

}  // namespace nexuskv

#ifdef __cplusplus
extern "C" {
#endif

typedef void* nexuskv_client_t;

inline nexuskv_client_t nexuskv_client_create(const char* control_plane_addr) {
    std::string addr = control_plane_addr ? control_plane_addr : "127.0.0.1:9098";
    return reinterpret_cast<nexuskv_client_t>(new nexuskv::NexusKVClient(addr));
}

inline void nexuskv_client_destroy(nexuskv_client_t client) {
    if (client) {
        delete reinterpret_cast<nexuskv::NexusKVClient*>(client);
    }
}

inline void nexuskv_client_get_metrics(nexuskv_client_t client, uint64_t* out_requests,
                                       uint64_t* out_hits) {
    if (client) {
        auto metrics = reinterpret_cast<nexuskv::NexusKVClient*>(client)->GetMetrics();
        if (out_requests) *out_requests = metrics.total_routing_requests;
        if (out_hits) *out_hits = metrics.total_cache_hits;
    }
}

#ifdef __cplusplus
}
#endif

#endif  // NEXUSKV_CLIENT_H
