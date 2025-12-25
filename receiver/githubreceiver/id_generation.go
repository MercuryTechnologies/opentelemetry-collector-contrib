// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package githubreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/githubreceiver"

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// idGenerator defines the interface for generating deterministic trace and span IDs.
type idGenerator interface {
	// generateTraceID creates a deterministic trace ID for a workflow run.
	generateTraceID(runID int64, runAttempt int) (pcommon.TraceID, error)
	// generateParentSpanID creates a deterministic parent span ID for a workflow.
	generateParentSpanID(runID int64, runAttempt int) (pcommon.SpanID, error)
	// generateJobSpanID creates a deterministic span ID for a job.
	generateJobSpanID(runID int64, runAttempt int, jobName string) (pcommon.SpanID, error)
	// generateStepSpanID creates a deterministic span ID for a step.
	generateStepSpanID(runID int64, runAttempt int, jobName, stepIdentifier string, stepNumber int) (pcommon.SpanID, error)
}

// newIDGenerator creates an ID generator based on the configured generation mode.
func newIDGenerator(mode IDGeneration) idGenerator {
	switch mode {
	case IDGenerationGitHubContext:
		return &githubContextIDGenerator{}
	default:
		return &legacyIDGenerator{}
	}
}

// hashToTraceID deterministically maps an arbitrary string to a TraceID.
func hashToTraceID(input string) (pcommon.TraceID, error) {
	hash := sha256.Sum256([]byte(input))
	idHex := hex.EncodeToString(hash[:])
	var id pcommon.TraceID
	_, err := hex.Decode(id[:], []byte(idHex[:32]))
	if err != nil {
		return pcommon.TraceID{}, err
	}
	return id, nil
}

// hashToSpanID deterministically maps an arbitrary string to a SpanID.
func hashToSpanID(input string) (pcommon.SpanID, error) {
	hash := sha256.Sum256([]byte(input))
	spanIDHex := hex.EncodeToString(hash[:])
	var spanID pcommon.SpanID
	_, err := hex.Decode(spanID[:], []byte(spanIDHex[16:32]))
	if err != nil {
		return pcommon.SpanID{}, err
	}
	return spanID, nil
}

// legacyIDGenerator implements the original ID generation algorithm.
// It uses run_id + run_attempt as the basis for all IDs.
type legacyIDGenerator struct{}

func (g *legacyIDGenerator) generateTraceID(runID int64, runAttempt int) (pcommon.TraceID, error) {
	input := fmt.Sprintf("%d%dt", runID, runAttempt)
	return hashToTraceID(input)
}

func (g *legacyIDGenerator) generateParentSpanID(runID int64, runAttempt int) (pcommon.SpanID, error) {
	input := fmt.Sprintf("%d%ds", runID, runAttempt)
	return hashToSpanID(input)
}

func (g *legacyIDGenerator) generateJobSpanID(runID int64, runAttempt int, jobName string) (pcommon.SpanID, error) {
	input := fmt.Sprintf("%d%d%s", runID, runAttempt, jobName)
	return hashToSpanID(input)
}

func (g *legacyIDGenerator) generateStepSpanID(runID int64, runAttempt int, jobName, stepIdentifier string, stepNumber int) (pcommon.SpanID, error) {
	// Legacy mode uses the original step name + number
	input := fmt.Sprintf("%d%d%s%s%d", runID, runAttempt, jobName, stepIdentifier, stepNumber)
	return hashToSpanID(input)
}

// githubContextIDGenerator implements a GitHub context-inspired ID generation algorithm.
// It uses workflow_job.id (which aligns with check run ID) for more stable identifiers
// that better align with GitHub's job execution context.
//
// Note: This mode approximates github.action naming by normalizing step names from
// webhook payloads. It does not attempt to match exact runner behavior like
// __run/__run_2 naming, as those details are not available in webhook payloads.
type githubContextIDGenerator struct{}

func (g *githubContextIDGenerator) generateTraceID(runID int64, runAttempt int) (pcommon.TraceID, error) {
	// In github_context mode, runID is actually the job.id (check run ID)
	input := fmt.Sprintf("%d%dt", runID, runAttempt)
	return hashToTraceID(input)
}

func (g *githubContextIDGenerator) generateParentSpanID(runID int64, runAttempt int) (pcommon.SpanID, error) {
	// In github_context mode, runID is actually the job.id (check run ID)
	input := fmt.Sprintf("%d%ds", runID, runAttempt)
	return hashToSpanID(input)
}

func (g *githubContextIDGenerator) generateJobSpanID(runID int64, runAttempt int, jobName string) (pcommon.SpanID, error) {
	// In github_context mode, runID is actually the job.id (check run ID)
	input := fmt.Sprintf("%d%d%s", runID, runAttempt, jobName)
	return hashToSpanID(input)
}

func (g *githubContextIDGenerator) generateStepSpanID(runID int64, runAttempt int, jobName, stepIdentifier string, stepNumber int) (pcommon.SpanID, error) {
	// In github_context mode:
	// - runID is the job.id (check run ID)
	// - stepIdentifier is the normalized action-like name
	// - stepNumber is ignored (not part of GitHub context)
	actionLike := normalizeGitHubActionLikeID(stepIdentifier)
	input := fmt.Sprintf("%d%d%s%s", runID, runAttempt, jobName, actionLike)
	return hashToSpanID(input)
}

// normalizeGitHubActionLikeID approximates GitHub's github.action naming behavior.
// GitHub documents that special characters are removed from github.action values.
// This provides a stable, deterministic identifier from step names available in webhooks.
//
// Note: This does not reproduce exact runner naming like __run/__run_2, as those
// internal identifiers are not exposed in webhook payloads.
var nonActionChars = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

func normalizeGitHubActionLikeID(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	s = strings.ToLower(s)
	return nonActionChars.ReplaceAllString(s, "")
}
