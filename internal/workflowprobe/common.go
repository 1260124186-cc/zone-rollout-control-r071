package workflowprobe

import (
	"net/http"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

const leaseDurationUnit = time.Second

func enrollNode(harness *harness, name string, region string) (domain.ResolverNode, error) {
	var node domain.ResolverNode
	err := harness.call(http.MethodPut, "/v1/nodes", domain.NodeCreateInput{
		NodeName: name,
		Region:   region,
	}, &node)
	return node, err
}

func createActivePlan(
	harness *harness,
	zone string,
	serial uint32,
	expectedNodes int,
	minimumRatio float64,
	maximumLatency int,
) (domain.RolloutPlan, domain.ZoneRevision, error) {
	var plan domain.RolloutPlan
	err := harness.call(http.MethodPut, "/v1/plans", domain.PlanCreateInput{
		Zone:                zone,
		MinimumSuccessRatio: minimumRatio,
		MaximumLatencyMS:    maximumLatency,
		ExpectedNodes:       expectedNodes,
	}, &plan)
	if err != nil {
		return domain.RolloutPlan{}, domain.ZoneRevision{}, err
	}
	var revision domain.ZoneRevision
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions", domain.RevisionCreateInput{
		Serial:    serial,
		Authority: "ns1." + zone,
		Entries: []domain.ZoneEntry{
			{Owner: zone, Type: domain.EntryA, Value: "192.0.2.10"},
			{Owner: "www." + zone, Type: domain.EntryCNAME, Value: zone},
		},
	}, &revision)
	if err != nil {
		return domain.RolloutPlan{}, domain.ZoneRevision{}, err
	}
	plan.Version++
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/activate", map[string]any{
		"expected_version": plan.Version,
	}, &plan); err != nil {
		return domain.RolloutPlan{}, domain.ZoneRevision{}, err
	}
	return plan, revision, nil
}

func allocate(
	harness *harness,
	planID string,
	requestKey string,
	region string,
	maxNodes int,
) (domain.LeaseGroup, error) {
	var group domain.LeaseGroup
	err := harness.call(http.MethodPatch, "/v1/plans/"+planID+"/leases", domain.LeaseInput{
		RequestKey:   requestKey,
		Region:       region,
		LeaseSeconds: 600,
		MaxNodes:     maxNodes,
	}, &group)
	return group, err
}

func submitProbe(
	harness *harness,
	leaseID string,
	requestKey string,
	success bool,
	serial uint32,
	latency int,
) (domain.Probe, error) {
	var result domain.Probe
	err := harness.call(http.MethodPatch, "/v1/leases/"+leaseID+"/probes", domain.ProbeInput{
		RequestKey:     requestKey,
		Success:        success,
		ObservedSerial: serial,
		LatencyMS:      latency,
		ObservedAt:     harness.clock.Now(),
	}, &result)
	return result, err
}

func createDraftPlan(
	harness *harness,
	zone string,
	serial uint32,
	expectedNodes int,
) (domain.RolloutPlan, domain.ZoneRevision, error) {
	var plan domain.RolloutPlan
	err := harness.call(http.MethodPut, "/v1/plans", domain.PlanCreateInput{
		Zone:                zone,
		MinimumSuccessRatio: 1,
		MaximumLatencyMS:    100,
		ExpectedNodes:       expectedNodes,
	}, &plan)
	if err != nil {
		return domain.RolloutPlan{}, domain.ZoneRevision{}, err
	}
	var revision domain.ZoneRevision
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions", domain.RevisionCreateInput{
		Serial:    serial,
		Authority: "ns1." + zone,
		Entries: []domain.ZoneEntry{
			{Owner: zone, Type: domain.EntryA, Value: "192.0.2.10"},
		},
	}, &revision)
	if err != nil {
		return domain.RolloutPlan{}, domain.ZoneRevision{}, err
	}
	return plan, revision, nil
}

func activateDraft(
	harness *harness,
	plan domain.RolloutPlan,
	serial uint32,
) (domain.RolloutPlan, domain.ZoneRevision, error) {
	plan.Version++
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/activate", map[string]any{
		"expected_version": plan.Version,
	}, &plan); err != nil {
		return domain.RolloutPlan{}, domain.ZoneRevision{}, err
	}
	return plan, domain.ZoneRevision{ID: plan.LatestRevisionID, PlanID: plan.ID, Serial: serial}, nil
}

func completePlan(harness *harness, plan domain.RolloutPlan) error {
	var completed domain.RolloutPlan
	return harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/complete", map[string]any{
		"expected_version": plan.Version,
	}, &completed)
}

func allocateWithSeconds(
	harness *harness,
	planID string,
	requestKey string,
	region string,
	maxNodes int,
	leaseSeconds int,
) (domain.LeaseGroup, error) {
	var group domain.LeaseGroup
	err := harness.call(http.MethodPatch, "/v1/plans/"+planID+"/leases", domain.LeaseInput{
		RequestKey:   requestKey,
		Region:       region,
		LeaseSeconds: leaseSeconds,
		MaxNodes:     maxNodes,
	}, &group)
	return group, err
}
