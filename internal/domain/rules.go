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
		NodeID:    lease.NodeID,
		PlanID:    lease.PlanID,
		LeaseID:   lease.ID,
		ClaimedAt: lease.ClaimedAt.UTC(),
		ExpiresAt: lease.ExpiresAt.UTC(),
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

// ExpireOccupancy marks claimed leases whose deadline has passed as expired,
// returns their nodes to ready and drops their occupancy records.
func ExpireOccupancy(state *State, now time.Time) int {
	count := 0
	now = now.UTC()
	for leaseID, lease := range state.Leases {
		if lease.State != LeaseClaimed || lease.ExpiresAt.After(now) {
			continue
		}
		finishedAt := now
		lease.State = LeaseExpired
		lease.FinishedAt = &finishedAt
		state.Leases[leaseID] = lease
		if node, ok := state.Nodes[lease.NodeID]; ok && node.State == NodeLeased {
			node.State = NodeReady
			node.Version++
			node.UpdatedAt = now
			state.Nodes[node.ID] = node
		}
		if claim, claimed := state.NodeClaims[lease.NodeID]; claimed && claim.LeaseID == leaseID {
			delete(state.NodeClaims, lease.NodeID)
		}
		count++
	}
	return count
}

// ReconcileOccupancy rebuilds every occupancy observation from lease truth.
// Due leases expire first; the claim set is then rebuilt strictly from live
// claimed leases and node states are repaired to match it. Occupancy left
// behind by requests that were never accepted (claims or leased nodes without
// a claimed lease) is removed, while leases that are still claimed keep
// holding their nodes. Quarantined nodes are left in quarantine. It reports
// whether the state changed.
func ReconcileOccupancy(state *State, now time.Time) bool {
	changed := ExpireOccupancy(state, now) > 0
	now = now.UTC()

	live := BuildClaimsFromLeases(state.Leases)
	if state.NodeClaims == nil {
		state.NodeClaims = make(map[string]NodeClaim)
	}
	for nodeID := range state.NodeClaims {
		if _, ok := live[nodeID]; !ok {
			delete(state.NodeClaims, nodeID)
			changed = true
		}
	}
	for nodeID, claim := range live {
		existing, ok := state.NodeClaims[nodeID]
		if !ok || existing != claim {
			state.NodeClaims[nodeID] = claim
			changed = true
		}
	}

	for nodeID, node := range state.Nodes {
		if node.State == NodeQuarantined {
			continue
		}
		_, occupied := live[nodeID]
		switch {
		case occupied && node.State != NodeLeased:
			node.State = NodeLeased
			node.Version++
			node.UpdatedAt = now
			state.Nodes[nodeID] = node
			changed = true
		case !occupied && node.State != NodeReady:
			node.State = NodeReady
			node.Version++
			node.UpdatedAt = now
			state.Nodes[nodeID] = node
			changed = true
		}
	}
	return changed
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
