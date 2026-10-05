package allocation

import (
	"context"
	"sort"
	"strings"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/store"
)

type Service struct {
	store *store.Store
	clock platform.Clock
	ids   platform.IDGenerator
}

func New(stateStore *store.Store, clock platform.Clock, ids platform.IDGenerator) *Service {
	return &Service{store: stateStore, clock: clock, ids: ids}
}

func (s *Service) Allocate(
	ctx context.Context,
	planID string,
	input domain.LeaseInput,
) (domain.LeaseGroup, error) {
	if err := domain.ValidateLeaseInput(input); err != nil {
		return domain.LeaseGroup{}, err
	}
	input.Region = strings.TrimSpace(strings.ToLower(input.Region))
	requestIndex := leaseRequestIndex(planID, input.RequestKey)
	fingerprint := domain.LeaseFingerprint(planID, input)
	now := s.clock.Now().UTC()
	replayed, found, err := s.replayGroup(ctx, requestIndex, fingerprint)
	if err != nil {
		return domain.LeaseGroup{}, err
	}
	if found {
		return replayed, nil
	}

	var group domain.LeaseGroup
	err = s.store.Write(ctx, func(state *domain.State) error {
		plan, ok := state.Plans[planID]
		if !ok {
			return domain.NotFound("plan", planID)
		}
		if plan.State != domain.PlanActive {
			return domain.InvalidState("plan must be active for node leasing", map[string]any{
				"plan_id": planID,
				"state":   plan.State,
			})
		}
		if existingID, exists := state.LeaseRequestKeys[requestIndex]; exists {
			existing, found := state.LeaseGroups[existingID]
			if !found {
				return domain.NewError(domain.CodeStorageFailure, "lease request index is inconsistent", map[string]any{
					"lease_group_id": existingID,
				})
			}
			if existing.RequestFingerprint != fingerprint {
				return domain.NewError(domain.CodeIdempotencyConflict, "request key was reused with different lease parameters", map[string]any{
					"request_key": input.RequestKey,
				})
			}
			group = existing
			return nil
		}

		expireLeases(state, now)

		claims := s.selectClaims(state, planID, input, now)
		if len(claims) == 0 {
			// No state has been mutated at this point, so a rejected request
			// leaves neither node occupancy nor half-written leases behind.
			return domain.NewError(domain.CodeNodeUnavailable, "no eligible node is ready", map[string]any{
				"plan_id": planID,
				"region":  input.Region,
			})
		}

		group = s.commitClaims(state, planID, input, fingerprint, claims, now)
		return nil
	})
	return group, err
}

// selectClaims picks the ready nodes an allocation may occupy and builds the
// claims it is about to take. It mutates nothing; the caller commits the result
// inside the same transaction, so a later rejection cannot leave occupancy.
func (s *Service) selectClaims(
	state *domain.State,
	planID string,
	input domain.LeaseInput,
	now time.Time,
) []domain.NodeClaim {
	candidates := make([]domain.ResolverNode, 0)
	for _, node := range state.Nodes {
		if node.State != domain.NodeReady {
			continue
		}
		if domain.NodeClaimBlocks(node.ID, state.NodeClaims) {
			continue
		}
		if input.Region != "" && node.Region != input.Region {
			continue
		}
		candidates = append(candidates, node)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Region != candidates[j].Region {
			return candidates[i].Region < candidates[j].Region
		}
		return candidates[i].ID < candidates[j].ID
	})
	if len(candidates) > input.MaxNodes {
		candidates = candidates[:input.MaxNodes]
	}
	expiresAt := now.Add(time.Duration(input.LeaseSeconds) * time.Second)
	claims := make([]domain.NodeClaim, 0, len(candidates))
	for _, node := range candidates {
		claims = append(claims, domain.NodeClaim{
			NodeID:     node.ID,
			PlanID:     planID,
			RequestKey: input.RequestKey,
			LeaseID:    s.ids.New("lse"),
			ClaimedAt:  now,
			ExpiresAt:  expiresAt,
		})
	}
	return claims
}

// replayGroup returns the group produced by an identical earlier request. A key
// that was reused with different parameters falls through so the conflict is
// reported by the commit path.
func (s *Service) replayGroup(
	ctx context.Context,
	requestIndex string,
	fingerprint string,
) (domain.LeaseGroup, bool, error) {
	var group domain.LeaseGroup
	found := false
	err := s.store.Read(ctx, func(state *domain.State) error {
		existingID, exists := state.LeaseRequestKeys[requestIndex]
		if !exists {
			return nil
		}
		existing, ok := state.LeaseGroups[existingID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "lease request index is inconsistent", map[string]any{
				"lease_group_id": existingID,
			})
		}
		if existing.RequestFingerprint != fingerprint {
			return nil
		}
		group = existing
		found = true
		return nil
	})
	return group, found, err
}

// commitClaims turns selected occupancy into leased nodes, leases and the lease
// group of one allocation request, all inside the caller's transaction.
func (s *Service) commitClaims(
	state *domain.State,
	planID string,
	input domain.LeaseInput,
	fingerprint string,
	claims []domain.NodeClaim,
	now time.Time,
) domain.LeaseGroup {
	groupID := s.ids.New("grp")
	leaseIDs := make([]string, 0, len(claims))
	for _, claim := range claims {
		leaseIDs = append(leaseIDs, claim.LeaseID)
		state.Leases[claim.LeaseID] = domain.NodeLease{
			ID:           claim.LeaseID,
			PlanID:       planID,
			NodeID:       claim.NodeID,
			AllocationID: groupID,
			State:        domain.LeaseClaimed,
			ClaimedAt:    claim.ClaimedAt,
			ExpiresAt:    claim.ExpiresAt,
		}
		state.NodeClaims[claim.NodeID] = claim
		if node, ok := state.Nodes[claim.NodeID]; ok && node.State == domain.NodeReady {
			node.State = domain.NodeLeased
			node.Version++
			node.UpdatedAt = now
			state.Nodes[node.ID] = node
		}
	}
	group := domain.LeaseGroup{
		ID:                 groupID,
		PlanID:             planID,
		RequestKey:         input.RequestKey,
		RequestFingerprint: fingerprint,
		Region:             input.Region,
		LeaseIDs:           leaseIDs,
		CreatedAt:          now,
	}
	state.LeaseGroups[group.ID] = group
	state.LeaseRequestKeys[leaseRequestIndex(planID, input.RequestKey)] = group.ID
	store.AddAudit(state, s.ids, "lease_group", group.ID, "created", now, map[string]any{
		"plan_id":     planID,
		"lease_count": len(leaseIDs),
	})
	return group
}

func leaseRequestIndex(planID string, requestKey string) string {
	return planID + "\x00" + requestKey
}

func (s *Service) Release(ctx context.Context, leaseID string) (domain.NodeLease, error) {
	now := s.clock.Now().UTC()
	var result domain.NodeLease
	err := s.store.Write(ctx, func(state *domain.State) error {
		lease, ok := state.Leases[leaseID]
		if !ok {
			return domain.NotFound("lease", leaseID)
		}
		if lease.State == domain.LeaseReleased || lease.State == domain.LeaseExpired {
			result = lease
			return nil
		}
		if lease.State != domain.LeaseClaimed {
			return domain.InvalidState("only a claimed lease can be released", map[string]any{
				"lease_id": leaseID,
				"state":    lease.State,
			})
		}
		node, ok := state.Nodes[lease.NodeID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "lease references a missing node", map[string]any{
				"node_id": lease.NodeID,
			})
		}
		finishedAt := now
		lease.State = domain.LeaseReleased
		lease.FinishedAt = &finishedAt
		state.Leases[leaseID] = lease
		if node.State == domain.NodeLeased {
			node.State = domain.NodeReady
			node.Version++
			node.UpdatedAt = now
			state.Nodes[node.ID] = node
		}
		if claim, claimed := state.NodeClaims[node.ID]; claimed && claim.LeaseID == leaseID {
			delete(state.NodeClaims, node.ID)
		}
		store.AddAudit(state, s.ids, "lease", leaseID, "released", now, nil)
		result = lease
		return nil
	})
	return result, err
}

func (s *Service) ExpireDue(ctx context.Context) (int, error) {
	now := s.clock.Now().UTC()
	expired := 0
	err := s.store.Write(ctx, func(state *domain.State) error {
		expired = expireLeases(state, now)
		return nil
	})
	return expired, err
}

func expireLeases(state *domain.State, now time.Time) int {
	return domain.ExpireOccupancy(state, now)
}
