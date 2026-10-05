package workflowprobe

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"solo-0014-zone-rollout-control/internal/allocation"
	"solo-0014-zone-rollout-control/internal/assessment"
	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/fleet"
	"solo-0014-zone-rollout-control/internal/httpapi"
	"solo-0014-zone-rollout-control/internal/plan"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/probe"
	"solo-0014-zone-rollout-control/internal/store"
)

type harness struct {
	baseURL  string
	clock    *platform.ManualClock
	client   *http.Client
	server   *http.Server
	listener net.Listener
	tempDir  string
	backing  *backing
}

type backing struct {
	store       *store.Store
	plans       *plan.Service
	nodes       *fleet.Service
	allocations *allocation.Service
	probes      *probe.Service
	assessments *assessment.Service
}

func newBacking(statePath string, clock platform.Clock) (*backing, error) {
	stateStore, err := store.Open(statePath, clock.Now())
	if err != nil {
		return nil, err
	}
	ids := platform.CryptoIDGenerator{}
	return &backing{
		store:       stateStore,
		plans:       plan.New(stateStore, clock, ids),
		nodes:       fleet.New(stateStore, clock, ids),
		allocations: allocation.New(stateStore, clock, ids),
		probes:      probe.New(stateStore, clock, ids, allocation.New(stateStore, clock, ids)),
		assessments: assessment.New(stateStore, clock, ids),
	}, nil
}

func newHarness() (*harness, error) {
	tempDir, err := os.MkdirTemp("", "zrc-probe-*")
	if err != nil {
		return nil, fmt.Errorf("create temporary directory: %w", err)
	}
	clock := platform.NewManualClock(time.Date(2026, time.September, 25, 12, 0, 0, 0, time.UTC))
	services, err := newBacking(tempDir+"/state.json", clock)
	if err != nil {
		_ = os.RemoveAll(tempDir)
		return nil, fmt.Errorf("open temporary store: %w", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	api := httpapi.New(services.plans, services.nodes, services.allocations, services.probes, services.assessments, platform.CryptoIDGenerator{}, logger)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = os.RemoveAll(tempDir)
		return nil, fmt.Errorf("listen: %w", err)
	}
	server := &http.Server{Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		_ = server.Serve(listener)
	}()
	return &harness{
		baseURL:  "http://" + listener.Addr().String(),
		clock:    clock,
		client:   &http.Client{Timeout: 10 * time.Second},
		server:   server,
		listener: listener,
		tempDir:  tempDir,
		backing:  services,
	}, nil
}

func (h *harness) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = h.server.Shutdown(ctx)
	_ = h.listener.Close()
	_ = os.RemoveAll(h.tempDir)
}

func (h *harness) statePath() string {
	return h.tempDir + "/state.json"
}

// mutateSnapshot edits the persisted JSON snapshot directly, simulating
// occupancy left on disk by an older service build or an abnormal shutdown.
func (h *harness) mutateSnapshot(fn func(*domain.State)) error {
	payload, err := os.ReadFile(h.statePath())
	if err != nil {
		return err
	}
	var state domain.State
	if err := json.Unmarshal(payload, &state); err != nil {
		return err
	}
	fn(&state)
	encoded, err := json.MarshalIndent(&state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(h.statePath(), append(encoded, '\n'), 0o600)
}

func (h *harness) call(method string, path string, requestBody any, target any) error {
	var payload []byte
	var err error
	if requestBody != nil {
		payload, err = json.Marshal(requestBody)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
	}
	request, err := http.NewRequest(method, h.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	if requestBody != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := h.client.Do(request)
	if err != nil {
		return fmt.Errorf("perform request: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var failure struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(body, &failure); err != nil {
			return fmt.Errorf("decode error response: %w", err)
		}
		return &apiFailure{
			Status:  response.StatusCode,
			Code:    failure.Error.Code,
			Message: failure.Error.Message,
		}
	}
	if target == nil {
		return nil
	}
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if err := json.Unmarshal(envelope.Data, target); err != nil {
		return fmt.Errorf("decode response data: %w", err)
	}
	return nil
}
