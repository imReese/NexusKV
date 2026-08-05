#ifndef NEXUSKV_CLIENT_H
#define NEXUSKV_CLIENT_H

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace nexuskv {

enum class ReuseDisposition { FULL_HIT, PARTIAL_HIT, MISS, BYPASS };

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
};

class NexusKVClient {
   public:
    NexusKVClient(std::string control_plane_addr = "127.0.0.1:9090")
        : control_plane_addr_(std::move(control_plane_addr)) {}

    RoutingDecision RoutePrefix(const std::vector<int>& prompt_tokens,
                                const std::vector<std::string>& candidate_workers) {
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
        decision.disposition = (decision.matched_token_count > 0) ? ReuseDisposition::PARTIAL_HIT
                                                                  : ReuseDisposition::MISS;
        return decision;
    }

    bool HandshakePrefillDecode(const std::string& request_id, const std::string& prefill_node,
                                const std::string& decode_node,
                                const std::vector<PinnedBlockHandle>& blocks) {
        return !request_id.empty() && !blocks.empty();
    }

   private:
    std::string control_plane_addr_;
};

}  // namespace nexuskv

#endif  // NEXUSKV_CLIENT_H
