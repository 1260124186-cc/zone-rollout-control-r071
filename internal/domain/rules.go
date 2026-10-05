package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"time"
)

func ActivatePlan(plan *RolloutPlan, now time.Time) error {
	if plan.State != PlanDraft {
		return InvalidState("plan can be activated only from draft", map[string]any{
			"plan_id": plan.ID,
			"state":   plan.State,
		})
	}
	if plan.LatestRevisionID == "" {
		return InvalidState("plan needs a zone revision before activation", map[string]any{
			"plan_id": plan.ID,
		})
	}
	plan.State = PlanActive
	plan.ActivatedAt = timePointer(now.UTC())
	plan.Version++
	return nil
}

func CompletePlan(plan *RolloutPlan, now time.Time) error {
	if plan.State != PlanActive {
		return InvalidState("plan can be completed only from active", map[string]any{
			"plan_id": plan.ID,
			"state":   plan.State,
		})
	}
	plan.State = PlanCompleted
	plan.CompletedAt = timePointer(now.UTC())
	plan.Version++
	return nil
}

func LeaseFingerprint(planID string, input LeaseInput) string {
	payload := fmt.Sprintf(
		"%s|%s|%s|%d|%d",
		planID,
		input.RequestKey,
		input.Region,
		input.LeaseSeconds,
		input.MaxNodes,
	)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

// NodeClaimForLease derives the occupancy a committed lease takes on its node.
func NodeClaimForLease(lease NodeLease) NodeClaim {
	return NodeClaim{
		NodeID:     lease.NodeID,
		PlanID:     lease.PlanID,
		RequestKey: "",
		LeaseID:    lease.ID,
		ClaimedAt:  lease.ClaimedAt.UTC(),
		ExpiresAt:  lease.ExpiresAt.UTC(),
	}
}

// NodeClaimBlocks reports whether a node still carries an occupancy record.
func NodeClaimBlocks(nodeID string, claims map[string]NodeClaim) bool {
	_, blocked := claims[nodeID]
	return blocked
}

// BuildClaimsFromLeases collects the occupancy supported by live leases. A
// lease that already reached a terminal state no longer holds its node.
func BuildClaimsFromLeases(leases map[string]NodeLease) map[string]NodeClaim {
	claims := make(map[string]NodeClaim)
	for _, lease := range leases {
		if lease.State != LeaseClaimed {
			continue
		}
		claims[lease.NodeID] = NodeClaimForLease(lease)
	}
	return claims
}

// ExpireClaimedLeases marks claimed leases whose deadline has passed as
// expired, returning the updated leases together with the node IDs whose
// occupancy ended this way.
func ExpireClaimedLeases(
	leases map[string]NodeLease,
	now time.Time,
) (map[string]NodeLease, []string) {
	freed := make([]string, 0)
	for leaseID, lease := range leases {
		if lease.State != LeaseClaimed || lease.ExpiresAt.After(now) {
			continue
		}
		finishedAt := now.UTC()
		lease.State = LeaseExpired
		lease.FinishedAt = &finishedAt
		leases[leaseID] = lease
		freed = append(freed, lease.NodeID)
	}
	return leases, freed
}

// ReconcileOccupancy rebuilds node occupancy strictly from lease facts. Every
// occupancy a request left behind without an accepted live lease is removed:
//
//   - claimed leases past their deadline are expired and release their nodes;
//   - the node claim set is replaced by the projections of live claimed
//     leases, so rejected requests can no longer block nodes;
//   - a node marked leased without a live claim returns to ready, while a
//     ready node that is in fact covered by a live claimed lease is marked
//     leased; quarantined nodes are never touched.
//
// The number of occupancy records removed without a backing lease is
// returned so callers can record the cleanup.
func ReconcileOccupancy(state *State, now time.Time) int {
	now = now.UTC()
	liveLeases, _ := ExpireClaimedLeases(state.Leases, now)
	liveClaims := BuildClaimsFromLeases(liveLeases)
	orphaned := 0
	for nodeID := range state.NodeClaims {
		if _, covered := liveClaims[nodeID]; !covered {
			orphaned++
		}
	}
	state.NodeClaims = liveClaims
	for nodeID, node := range state.Nodes {
		_, occupied := liveClaims[nodeID]
		switch {
		case node.State == NodeLeased && !occupied:
			node.State = NodeReady
			node.Version++
			node.UpdatedAt = now
			state.Nodes[nodeID] = node
		case node.State == NodeReady && occupied:
			node.State = NodeLeased
			node.Version++
			node.UpdatedAt = now
			state.Nodes[nodeID] = node
		}
	}
	return orphaned
}

func ProbeFingerprint(leaseID string, input ProbeInput) string {
	payload := fmt.Sprintf(
		"%s|%s|%t|%d|%d|%s",
		leaseID,
		input.RequestKey,
		input.Success,
		input.ObservedSerial,
		input.LatencyMS,
		input.ObservedAt.UTC().Format(time.RFC3339Nano),
	)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func BuildAssessment(
	id string,
	plan RolloutPlan,
	revision ZoneRevision,
	probes []Probe,
	now time.Time,
) RolloutAssessment {
	reasons := make(map[string]struct{})
	if len(probes) == 0 {
		reasons["no_probes"] = struct{}{}
	}
	probeCount := len(probes)
	successCount := 0
	maximumLatency := 0
	nodes := make(map[string]struct{})
	for _, probe := range probes {
		nodes[probe.NodeID] = struct{}{}
		if probe.LatencyMS > maximumLatency {
			maximumLatency = probe.LatencyMS
		}
		if probe.Success {
			successCount++
			if probe.ObservedSerial != revision.Serial {
				reasons["serial_mismatch"] = struct{}{}
			}
		}
	}
	if len(nodes) < plan.ExpectedNodes {
		reasons["node_not_observed"] = struct{}{}
	}
	successRatio := 0.0
	if probeCount > 0 {
		successRatio = round(float64(successCount)/float64(probeCount), 6)
	}
	if successRatio < plan.MinimumSuccessRatio {
		reasons["success_ratio_below_minimum"] = struct{}{}
	}
	if maximumLatency > plan.MaximumLatencyMS {
		reasons["latency_limit_exceeded"] = struct{}{}
	}
	reasonList := make([]string, 0, len(reasons))
	for reason := range reasons {
		reasonList = append(reasonList, reason)
	}
	sort.Strings(reasonList)
	verdict := VerdictConsistent
	if len(reasonList) > 0 {
		verdict = VerdictReviewRequired
	}
	return RolloutAssessment{
		ID:               id,
		PlanID:           plan.ID,
		RevisionID:       revision.ID,
		Verdict:          verdict,
		Reasons:          reasonList,
		ProbeCount:       probeCount,
		SuccessCount:     successCount,
		SuccessRatio:     successRatio,
		MaximumLatencyMS: maximumLatency,
		CreatedAt:        now.UTC(),
	}
}

func round(value float64, places int) float64 {
	scale := math.Pow10(places)
	return math.Round(value*scale) / scale
}

func timePointer(value time.Time) *time.Time {
	return &value
}
