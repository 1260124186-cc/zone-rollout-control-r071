package workflowprobe

import (
	"context"
	"net/http"

	"solo-0014-zone-rollout-control/internal/domain"
)

// runOccupancyRecovery guards the occupancy lifecycle fixes:
//
//   - a lease request that is rejected leaves neither leased nodes, claims,
//     leases nor request-key traces;
//   - reporting, releasing and expiring a lease end the occupancy for good and
//     the node can be claimed by another plan without a restart;
//   - restarting re-derives occupancy from lease facts: ghost occupancy left by
//     rejected requests is cleaned while live leases keep their exclusivity.
func runOccupancyRecovery() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()
	ctx := context.Background()

	nodeA, err := enrollNode(harness, "resolver-recover-01", "east")
	if err != nil {
		return err
	}
	nodeB, err := enrollNode(harness, "resolver-recover-02", "east")
	if err != nil {
		return err
	}

	// A lease attempted on a draft plan is rejected and must leave no trace.
	draft, _, err := createDraftPlan(harness, "recover-draft.example", 5001, 2)
	if err != nil {
		return err
	}
	if _, err := allocate(harness, draft.ID, "recover-draft-lease-01", "east", 2); err == nil {
		return ensure(false, "draft plan accepted a lease request")
	} else if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	if err := expectNodeState(harness, nodeA.ID, domain.NodeReady); err != nil {
		return err
	}
	if err := expectNodeState(harness, nodeB.ID, domain.NodeReady); err != nil {
		return err
	}
	if err := expectNoOccupancy(harness, nodeA.ID, nodeB.ID); err != nil {
		return err
	}

	// Activate and really allocate. The accepted leases must hold both nodes.
	plan, revision, err := activateDraft(harness, draft, 5001)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "recover-lease-0001", "east", 2)
	if err != nil {
		return err
	}
	if len(group.LeaseIDs) != 2 {
		return ensure(false, "accepted allocation did not lease both nodes")
	}
	if err := expectNodeState(harness, nodeA.ID, domain.NodeLeased); err != nil {
		return err
	}
	if err := expectNodeState(harness, nodeB.ID, domain.NodeLeased); err != nil {
		return err
	}
	// Same key with different parameters must conflict without disturbing
	// the accepted leases.
	if _, err := allocateWithSeconds(harness, plan.ID, "recover-lease-0001", "east", 2, 900); err == nil {
		return ensure(false, "same request key with different payload was accepted")
	} else if err := expectFailure(err, http.StatusConflict, string(domain.CodeIdempotencyConflict)); err != nil {
		return err
	}
	if err := expectNodeState(harness, nodeA.ID, domain.NodeLeased); err != nil {
		return err
	}

	// Both nodes report with probe request keys that intentionally differ from
	// the lease request key. Occupancy must end even though the keys differ.
	if _, err := submitProbe(harness, group.LeaseIDs[0], "recover-probe-0001", true, revision.Serial, 10); err != nil {
		return err
	}
	if _, err := submitProbe(harness, group.LeaseIDs[1], "recover-probe-0002", true, revision.Serial, 12); err != nil {
		return err
	}
	if err := expectNodeState(harness, nodeA.ID, domain.NodeReady); err != nil {
		return err
	}
	if err := expectNodeState(harness, nodeB.ID, domain.NodeReady); err != nil {
		return err
	}
	if err := expectNoOccupancy(harness, nodeA.ID, nodeB.ID); err != nil {
		return err
	}

	// Finish the plan; a re-issued lease against the completed plan must be
	// rejected while nodes stay free.
	if err := completePlan(harness, plan); err != nil {
		return err
	}
	if _, err := allocate(harness, plan.ID, "recover-late-lease-01", "east", 2); err == nil {
		return ensure(false, "completed plan accepted a lease request")
	} else if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	if err := expectNodeState(harness, nodeA.ID, domain.NodeReady); err != nil {
		return err
	}
	if err := expectNoOccupancy(harness, nodeA.ID, nodeB.ID); err != nil {
		return err
	}

	// A fresh active plan must be able to claim the nodes immediately, with no
	// restart in between.
	nextPlan, nextRevision, err := createActivePlan(harness, "recover-next.example", 5002, 1, 1, 100)
	if err != nil {
		return err
	}
	nextGroup, err := allocate(harness, nextPlan.ID, "recover-next-lease-01", "east", 1)
	if err != nil {
		return err
	}
	leasedNodeID := leasedNode(harness, nextGroup)
	if _, err := submitProbe(harness, nextGroup.LeaseIDs[0], "recover-next-probe-01", true, nextRevision.Serial, 8); err != nil {
		return err
	}
	if err := expectNodeState(harness, leasedNodeID, domain.NodeReady); err != nil {
		return err
	}

	// A second plan takes a short lease; advancing the clock past its deadline
	// and triggering the expiry sweep must free the node.
	expirePlan, _, err := createActivePlan(harness, "recover-expire.example", 5003, 1, 1, 100)
	if err != nil {
		return err
	}
	expireGroup, err := allocate(harness, expirePlan.ID, "recover-expire-lease-01", "east", 1)
	if err != nil {
		return err
	}
	expiringNodeID := leasedNode(harness, expireGroup)
	if _, err := harness.backing.allocations.ExpireDue(ctx); err != nil {
		return err
	}
	if err := expectNodeState(harness, expiringNodeID, domain.NodeLeased); err != nil {
		return err
	}
	harness.clock.Advance(601 * leaseDurationUnit)
	if _, err := harness.backing.allocations.ExpireDue(ctx); err != nil {
		return err
	}
	if err := expectNodeState(harness, expiringNodeID, domain.NodeReady); err != nil {
		return err
	}
	if err := expectNoOccupancy(harness, expiringNodeID); err != nil {
		return err
	}

	// Simulate an old, rejected request that persisted occupancy on disk: the
	// free node is flagged leased and carries a claim without any lease. Also
	// put a live lease on the other node to prove reconciliation preserves it.
	livePlan, _, err := createActivePlan(harness, "recover-live.example", 5004, 1, 1, 100)
	if err != nil {
		return err
	}
	liveGroup, err := allocate(harness, livePlan.ID, "recover-live-lease-01", "east", 1)
	if err != nil {
		return err
	}
	liveLeaseID := liveGroup.LeaseIDs[0]
	liveNodeID := leasedNode(harness, liveGroup)
	ghostNodeID := otherNode(nodeA.ID, nodeB.ID, liveNodeID)
	if err := harness.mutateSnapshot(func(state *domain.State) {
		node := state.Nodes[ghostNodeID]
		node.State = domain.NodeLeased
		node.Version++
		state.Nodes[ghostNodeID] = node
		state.NodeClaims[ghostNodeID] = domain.NodeClaim{
			NodeID:     ghostNodeID,
			PlanID:     "pln_rejected_ghost",
			RequestKey: "recover-rejected-ghost",
			LeaseID:    "lse_rejected_ghost",
			ClaimedAt:  harness.clock.Now(),
			ExpiresAt:  harness.clock.Now().Add(3600 * leaseDurationUnit),
		}
	}); err != nil {
		return err
	}

	// Restart: opening the snapshot re-derives occupancy from lease facts. The
	// ghost occupancy is discarded and persisted away, while the live lease
	// keeps holding its node and remains safely replayable and releasable.
	reopenedServices, err := newBacking(harness.statePath(), harness.clock)
	if err != nil {
		return err
	}
	postRestart, err := reopenedServices.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	if postRestart.Nodes[ghostNodeID].State != domain.NodeReady {
		return ensure(false, "ghost occupancy survived restart")
	}
	if _, blocked := postRestart.NodeClaims[ghostNodeID]; blocked {
		return ensure(false, "ghost node claim survived restart")
	}
	if postRestart.Nodes[liveNodeID].State != domain.NodeLeased {
		return ensure(false, "live lease lost its node on restart")
	}
	if postRestart.Leases[liveLeaseID].State != domain.LeaseClaimed {
		return ensure(false, "live lease was not preserved as claimed on restart")
	}
	if claim, blocked := postRestart.NodeClaims[liveNodeID]; !blocked || claim.LeaseID != liveLeaseID {
		return ensure(false, "live lease claim was not rebuilt on restart")
	}
	if _, present := postRestart.Leases["lse_rejected_ghost"]; present {
		return ensure(false, "a half lease was materialized on restart")
	}
	replay, err := reopenedServices.allocations.Allocate(ctx, livePlan.ID, domain.LeaseInput{
		RequestKey:   "recover-live-lease-01",
		Region:       "east",
		LeaseSeconds: 600,
		MaxNodes:     1,
	})
	if err != nil {
		return err
	}
	if replay.ID != liveGroup.ID {
		return ensure(false, "replaying the live lease after restart returned another group")
	}
	if len(replay.LeaseIDs) != 1 || replay.LeaseIDs[0] != liveLeaseID {
		return ensure(false, "replaying the live lease after restart returned another lease")
	}
	if err := releaseLease(reopenedServices, liveLeaseID); err != nil {
		return err
	}
	postRelease, err := reopenedServices.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	if postRelease.Nodes[liveNodeID].State != domain.NodeReady {
		return ensure(false, "released node did not return to ready after restart")
	}
	if _, blocked := postRelease.NodeClaims[liveNodeID]; blocked {
		return ensure(false, "released node kept a claim after restart")
	}
	return nil
}

func expectNodeState(harness *harness, nodeID string, want domain.NodeState) error {
	node, err := harness.backing.nodes.Get(context.Background(), nodeID)
	if err != nil {
		return err
	}
	if node.State != want {
		return ensure(false, "node "+nodeID+" state is "+string(node.State)+", expected "+string(want))
	}
	return nil
}

func expectNoOccupancy(harness *harness, nodeIDs ...string) error {
	snapshot, err := harness.backing.store.Snapshot(context.Background())
	if err != nil {
		return err
	}
	for _, nodeID := range nodeIDs {
		if _, claimed := snapshot.NodeClaims[nodeID]; claimed {
			return ensure(false, "node "+nodeID+" still carries an occupancy claim")
		}
	}
	return nil
}

func leasedNode(harness *harness, group domain.LeaseGroup) string {
	snapshot, _ := harness.backing.store.Snapshot(context.Background())
	return snapshot.Leases[group.LeaseIDs[0]].NodeID
}

func otherNode(first string, second string, picked string) string {
	if picked == first {
		return second
	}
	return first
}

func releaseLease(services *backing, leaseID string) error {
	released, err := services.allocations.Release(context.Background(), leaseID)
	if err != nil {
		return err
	}
	if released.State != domain.LeaseReleased {
		return ensure(false, "lease did not reach released state")
	}
	return nil
}
