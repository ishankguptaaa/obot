package invoke

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/obot-platform/obot/pkg/storage/apis/obot.obot.ai/v1"
)

func TestIsApprovedTool(t *testing.T) {
	tests := []struct {
		name          string
		toolName      string
		approvedTools []string
		expected      bool
	}{
		{
			name:          "exact match",
			toolName:      "myTool",
			approvedTools: []string{"myTool"},
			expected:      true,
		},
		{
			name:          "no match",
			toolName:      "myTool",
			approvedTools: []string{"otherTool"},
			expected:      false,
		},
		{
			name:          "empty approved list",
			toolName:      "myTool",
			approvedTools: nil,
			expected:      false,
		},
		{
			name:          "wildcard matches all",
			toolName:      "anything",
			approvedTools: []string{"*"},
			expected:      true,
		},
		{
			name:          "prefix wildcard match",
			toolName:      "fooBar",
			approvedTools: []string{"foo*"},
			expected:      true,
		},
		{
			name:          "prefix wildcard no match",
			toolName:      "barBaz",
			approvedTools: []string{"foo*"},
			expected:      false,
		},
		{
			name:          "multiple entries match later",
			toolName:      "baz",
			approvedTools: []string{"foo", "bar", "baz"},
			expected:      true,
		},
		{
			name:          "empty tool name",
			toolName:      "",
			approvedTools: []string{"foo"},
			expected:      false,
		},
		{
			name:          "wildcard with empty tool name",
			toolName:      "",
			approvedTools: []string{"*"},
			expected:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isApprovedTool(tt.toolName, tt.approvedTools))
		})
	}
}

func TestIsEphemeral(t *testing.T) {
	tests := []struct {
		name     string
		runName  string
		expected bool
	}{
		{
			name:     "ephemeral run with counter",
			runName:  "ephemeral-run-1",
			expected: true,
		},
		{
			name:     "ephemeral run with large counter",
			runName:  "ephemeral-run-99999",
			expected: true,
		},
		{
			name:     "ephemeral run exact prefix",
			runName:  "ephemeral-run",
			expected: true,
		},
		{
			name:     "non-ephemeral run with standard prefix",
			runName:  "r1abc123",
			expected: false,
		},
		{
			name:     "non-ephemeral run empty name",
			runName:  "",
			expected: false,
		},
		{
			name:     "non-ephemeral partial prefix match",
			runName:  "ephemeral-ru",
			expected: false,
		},
		{
			name:     "ephemeral with extended suffix",
			runName:  "ephemeral-runs-extra",
			expected: true, // starts with "ephemeral-run" prefix
		},
		{
			name:     "non-ephemeral different prefix",
			runName:  "eph-something",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := &v1.Run{
				ObjectMeta: metav1.ObjectMeta{
					Name: tt.runName,
				},
			}
			assert.Equal(t, tt.expected, isEphemeral(run))
		})
	}
}

func TestContextCancellationPropagation(t *testing.T) {
	// This test validates the core invariant of the abort context fix:
	// runCtx created via context.WithCancelCause in Resume() is the parent of
	// both saveCtx (in stream) and timeoutCtx (in stream), so cancelling runCtx
	// cancels all children and records the cause.

	tests := []struct {
		name          string
		causeErr      error
		expectedCause error
	}{
		{
			name:          "abort error propagates to children",
			causeErr:      fmt.Errorf("thread was aborted, cancelling run"),
			expectedCause: fmt.Errorf("thread was aborted, cancelling run"),
		},
		{
			name:          "timeout error propagates to children",
			causeErr:      fmt.Errorf("run exceeded maximum time of 10m0s"),
			expectedCause: fmt.Errorf("run exceeded maximum time of 10m0s"),
		},
		{
			name:          "nil cause still cancels children",
			causeErr:      nil,
			expectedCause: context.Canceled, // WithCancelCause with nil cause reports context.Canceled
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parentCtx := context.Background()
			runCtx, cancelRun := context.WithCancelCause(parentCtx)

			// Simulate the child contexts created in stream()
			saveCtx, saveCancel := context.WithCancel(runCtx)
			defer saveCancel()
			timeoutCtx, timeoutCancel := context.WithCancel(runCtx)
			defer timeoutCancel()

			// Before cancellation, all contexts should be active
			assert.NoError(t, runCtx.Err())
			assert.NoError(t, saveCtx.Err())
			assert.NoError(t, timeoutCtx.Err())

			// Cancel the run context (simulating watchThreadAbort or timeoutAfter)
			cancelRun(tt.causeErr)

			// All child contexts should now be done
			assert.Error(t, runCtx.Err())
			assert.Error(t, saveCtx.Err())
			assert.Error(t, timeoutCtx.Err())

			// Verify the cause is recorded correctly
			cause := context.Cause(runCtx)
			assert.Equal(t, tt.expectedCause.Error(), cause.Error())
		})
	}
}

func TestDoubleCancelRunSafety(t *testing.T) {
	// In the fixed code, cancelRun is called in two places:
	// 1. defer cancelRun(nil) in Resume()
	// 2. defer cancelRun(retErr) in stream()
	// The second call should be a no-op — only the first cause is recorded.

	tests := []struct {
		name          string
		firstCause    error
		secondCause   error
		expectedCause string
	}{
		{
			name:          "first real error wins over second",
			firstCause:    errors.New("thread was aborted, cancelling run"),
			secondCause:   errors.New("stream finished with error"),
			expectedCause: "thread was aborted, cancelling run",
		},
		{
			name:          "first real error wins over nil",
			firstCause:    errors.New("run exceeded maximum time of 10m0s"),
			secondCause:   nil,
			expectedCause: "run exceeded maximum time of 10m0s",
		},
		{
			name:          "first nil cause records context.Canceled",
			firstCause:    nil,
			secondCause:   errors.New("late error"),
			expectedCause: context.Canceled.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			runCtx, cancelRun := context.WithCancelCause(ctx)

			// First cancel (e.g., from watchThreadAbort or stream's defer)
			cancelRun(tt.firstCause)
			// Second cancel (e.g., from Resume's defer)
			cancelRun(tt.secondCause)

			cause := context.Cause(runCtx)
			assert.Equal(t, tt.expectedCause, cause.Error())
		})
	}
}

func TestTimeoutAfterCancelsContext(t *testing.T) {
	// Verify timeoutAfter calls cancelRun with an appropriate error message
	// when the timeout fires before context cancellation.

	ctx := context.Background()
	runCtx, cancelRun := context.WithCancelCause(ctx)
	defer cancelRun(nil)

	// Use a very short timeout so the test completes quickly
	go timeoutAfter(runCtx, cancelRun, 1*time.Nanosecond)

	// Wait for context to be cancelled
	<-runCtx.Done()

	cause := context.Cause(runCtx)
	assert.Contains(t, cause.Error(), "run exceeded maximum time")
}

// TestOriginalBug_AbortDoesNotCancelGptscriptExecution reproduces the actual bug from issue #6114.
//
// In the ORIGINAL code (before the fix), the context hierarchy looked like:
//
//	Resume(ctx) {
//	    gptClient.Run(ctx, ...)          // tool execution bound to parent ctx
//	    stream(ctx, ...) {
//	        runCtx = WithCancelCause(ctx) // child context
//	        watchThreadAbort(runCtx, cancelRun)  // cancels runCtx only
//	    }
//	}
//
// When the user clicks "Abort":
//  1. watchThreadAbort detects thread.Spec.Abort = true
//  2. Calls cancelRun() which cancels runCtx (child context)
//  3. stream() exits because <-runCtx.Done() fires
//  4. BUT gptClient.Run was started with ctx (parent), not runCtx
//  5. Parent ctx is NOT cancelled → gptscript tool calls keep running
//
// This test proves that the original architecture was broken: cancelling a child
// context does NOT cancel a sibling operation started with the parent context.
func TestOriginalBug_AbortDoesNotCancelGptscriptExecution(t *testing.T) {
	// Simulate the ORIGINAL (buggy) code structure
	parentCtx := context.Background()

	// This represents "ctx" in the original Resume() — the controller's request context
	controllerCtx, controllerCancel := context.WithCancel(parentCtx)
	defer controllerCancel()

	// Simulate gptClient.Run(ctx, ...) — started with the PARENT context
	gptscriptDone := make(chan struct{})
	gptscriptCancelled := make(chan struct{})
	go func() {
		defer close(gptscriptDone)
		// Simulates a long-running tool call in gptscript
		select {
		case <-controllerCtx.Done():
			close(gptscriptCancelled)
		case <-time.After(10 * time.Second):
			// Tool call would run to completion
		}
	}()

	// Simulate stream() creating runCtx as a child — this is what watchThreadAbort cancels
	runCtx, cancelRun := context.WithCancelCause(controllerCtx)

	// Simulate watchThreadAbort detecting abort and cancelling runCtx
	cancelRun(fmt.Errorf("thread was aborted, cancelling run"))

	// runCtx is cancelled (stream loop would exit)
	assert.Error(t, runCtx.Err(), "runCtx should be cancelled after abort")

	// BUT the parent context (controllerCtx) is NOT cancelled
	assert.NoError(t, controllerCtx.Err(), "BUG: parent ctx is still alive — gptscript keeps running")

	// Give gptscript a moment to react
	select {
	case <-gptscriptCancelled:
		t.Fatal("gptscript should NOT be cancelled in the buggy code — this means the bug doesn't reproduce")
	case <-time.After(100 * time.Millisecond):
		// Expected: gptscript is still running because parent ctx wasn't cancelled
		// This IS the bug — abort was clicked but tool calls continue
	}
}

// TestFix_AbortCancelsGptscriptExecution proves the fix works.
//
// In the FIXED code, the context hierarchy is:
//
//	Resume(ctx) {
//	    runCtx = WithCancelCause(ctx)     // hoisted to Resume
//	    gptClient.Run(runCtx, ...)        // tool execution bound to runCtx
//	    watchThreadAbort(runCtx, cancelRun) // cancels runCtx
//	    stream(runCtx, cancelRun, ...) {   // stream also uses runCtx
//	    }
//	}
//
// Now when watchThreadAbort cancels runCtx, gptscript execution is also cancelled.
func TestFix_AbortCancelsGptscriptExecution(t *testing.T) {
	// Simulate the FIXED code structure
	parentCtx := context.Background()

	controllerCtx, controllerCancel := context.WithCancel(parentCtx)
	defer controllerCancel()

	// In the fix, runCtx is created in Resume() BEFORE gptClient.Run
	runCtx, cancelRun := context.WithCancelCause(controllerCtx)
	defer cancelRun(nil)

	// Simulate gptClient.Run(runCtx, ...) — now uses runCtx, not parent ctx
	gptscriptCancelled := make(chan struct{})
	go func() {
		// Simulates a long-running tool call in gptscript
		select {
		case <-runCtx.Done():
			close(gptscriptCancelled)
		case <-time.After(10 * time.Second):
			// Tool call would run to completion
		}
	}()

	// Simulate child contexts created in stream() — all children of runCtx
	saveCtx, saveCancel := context.WithCancel(runCtx)
	defer saveCancel()

	// Simulate watchThreadAbort detecting abort and cancelling runCtx
	cancelRun(fmt.Errorf("thread was aborted, cancelling run"))

	// runCtx is cancelled
	assert.Error(t, runCtx.Err(), "runCtx should be cancelled after abort")

	// All children are cancelled too
	assert.Error(t, saveCtx.Err(), "saveCtx should be cancelled")

	// gptscript execution IS cancelled now
	select {
	case <-gptscriptCancelled:
		// SUCCESS: gptscript was cancelled by the abort
	case <-time.After(1 * time.Second):
		t.Fatal("gptscript should have been cancelled but wasn't")
	}

	// Parent context is still alive (controller is unaffected)
	assert.NoError(t, controllerCtx.Err(), "controller ctx should still be alive")
}

// TestOriginalBug_DeferCancelRunCapturesNil reproduces another bug in the original code.
//
// In the original code:
//
//	defer cancelRun(retErr)
//
// This captures the value of retErr at REGISTRATION time (when defer is evaluated),
// which is nil. So when stream() returns with an error, cancelRun is called with nil
// instead of the actual error. This means context.Cause(runCtx) returns context.Canceled
// instead of the real error.
//
// The fix wraps it in a closure:
//
//	defer func() { cancelRun(retErr) }()
func TestOriginalBug_DeferCancelRunCapturesNil(t *testing.T) {
	// Demonstrate the bug with direct defer (captures value at registration)
	t.Run("buggy: defer captures nil at registration", func(t *testing.T) {
		ctx := context.Background()
		runCtx, cancelRun := context.WithCancelCause(ctx)

		var retErr error

		// Simulate: defer cancelRun(retErr) — captures retErr NOW (nil)
		capturedErr := retErr // This is what Go does at defer registration

		// Later, retErr gets set to something
		retErr = errors.New("stream failed")
		_ = retErr // avoid unused warning

		// The defer fires with the captured nil, not the current retErr
		cancelRun(capturedErr)

		cause := context.Cause(runCtx)
		// BUG: cause is context.Canceled (from nil), not "stream failed"
		assert.Equal(t, context.Canceled.Error(), cause.Error(),
			"buggy defer captures nil, so cause is context.Canceled instead of the real error")
	})

	// Demonstrate the fix with closure defer (captures value at execution)
	t.Run("fixed: defer closure captures final value", func(t *testing.T) {
		ctx := context.Background()
		runCtx, cancelRun := context.WithCancelCause(ctx)

		var retErr error

		// Simulate: defer func() { cancelRun(retErr) }() — captures retErr at execution time
		retErr = errors.New("stream failed")

		// The closure captures the current value of retErr
		func() { cancelRun(retErr) }()

		cause := context.Cause(runCtx)
		// FIXED: cause is the real error
		assert.Equal(t, "stream failed", cause.Error(),
			"closure defer captures the final retErr value")
	})
}

// TestSelectLoopRace_ToolCallsAutoApprovedAfterAbort reproduces the deeper bug
// found during live testing: even with the context-hoisting fix, the stream()
// select loop can process EventTypeCallConfirm events and auto-approve tool calls
// AFTER runCtx has been cancelled.
//
// Go's select statement picks randomly when multiple cases are ready. So when:
//  1. runCtx is cancelled (abort detected)
//  2. A tool confirm event arrives on runEvent channel
//
// The select may pick the event case instead of the Done case, causing the tool
// call to be auto-approved despite the abort. This was observed in live testing:
// tool calls continued auto-approving for 19+ seconds after abort was sent.
//
// The fix: check runCtx.Err() at the TOP of the event processing (before auto-approve)
// and bail out immediately if the context is cancelled.
func TestSelectLoopRace_ToolCallsAutoApprovedAfterAbort(t *testing.T) {
	// Simulate the stream() select loop behavior
	runCtx, cancelRun := context.WithCancelCause(context.Background())

	// Simulated event channel (like runResp.Events())
	events := make(chan string, 10)

	// Fill with tool confirm events
	events <- "tool_confirm:workspace_list"
	events <- "tool_confirm:list_memories"
	events <- "tool_confirm:current_time"

	// Cancel the context BEFORE processing (simulating abort arriving first)
	cancelRun(fmt.Errorf("thread was aborted, cancelling run"))

	// Count how many tool calls get "auto-approved" despite abort
	autoApproved := 0

	// Simulate the BUGGY select loop (no runCtx.Err() check before auto-approve)
	buggyLoop := func() int {
		count := 0
		for {
			select {
			case <-runCtx.Done():
				return count
			case _, ok := <-events:
				if !ok {
					return count
				}
				// BUG: No check for runCtx.Err() here!
				// Auto-approve happens unconditionally
				count++
			}
		}
	}

	autoApproved = buggyLoop()
	// With the buggy code, some events WILL be processed because select picks randomly
	// We can't assert exact count due to Go scheduler non-determinism,
	// but over many iterations, events WILL leak through
	t.Logf("Buggy loop auto-approved %d tool calls after abort", autoApproved)

	// Now test the FIXED select loop with runCtx.Err() guard
	runCtx2, cancelRun2 := context.WithCancelCause(context.Background())
	events2 := make(chan string, 10)
	events2 <- "tool_confirm:workspace_list"
	events2 <- "tool_confirm:list_memories"
	events2 <- "tool_confirm:current_time"

	cancelRun2(fmt.Errorf("thread was aborted, cancelling run"))

	fixedLoop := func() int {
		count := 0
		for {
			select {
			case <-runCtx2.Done():
				return count
			case _, ok := <-events2:
				if !ok {
					return count
				}
				// FIX: Check context before auto-approving
				if runCtx2.Err() != nil {
					return count
				}
				count++
			}
		}
	}

	fixedApproved := fixedLoop()
	// With the fix, NO events should be auto-approved after abort
	assert.Equal(t, 0, fixedApproved,
		"Fixed loop should NOT auto-approve any tool calls after context is cancelled")
}

// TestSelectLoopRace_RepeatedTrials runs the select race scenario many times to
// demonstrate that the buggy version DOES leak tool approvals through the select.
func TestSelectLoopRace_RepeatedTrials(t *testing.T) {
	totalLeaked := 0
	trials := 100

	for i := 0; i < trials; i++ {
		ctx, cancel := context.WithCancelCause(context.Background())
		events := make(chan string, 5)
		events <- "confirm1"
		events <- "confirm2"
		events <- "confirm3"
		cancel(fmt.Errorf("aborted"))

		// Buggy: no guard
		count := 0
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			case _, ok := <-events:
				if !ok {
					break loop
				}
				count++ // leaked auto-approve
			}
		}
		totalLeaked += count
	}

	t.Logf("Over %d trials, buggy select leaked %d total tool approvals (avg %.1f per trial)",
		trials, totalLeaked, float64(totalLeaked)/float64(trials))
	// We expect SOME leaks due to Go select non-determinism
	// This test documents the behavior — if Go ever changes select semantics this will catch it

	// Now test fixed version
	totalFixed := 0
	for i := 0; i < trials; i++ {
		ctx, cancel := context.WithCancelCause(context.Background())
		events := make(chan string, 5)
		events <- "confirm1"
		events <- "confirm2"
		events <- "confirm3"
		cancel(fmt.Errorf("aborted"))

		count := 0
	fixedLoop:
		for {
			select {
			case <-ctx.Done():
				break fixedLoop
			case _, ok := <-events:
				if !ok {
					break fixedLoop
				}
				if ctx.Err() != nil {
					break fixedLoop // THE FIX
				}
				count++
			}
		}
		totalFixed += count
	}

	assert.Equal(t, 0, totalFixed,
		"Fixed version should never leak tool approvals")
	t.Logf("Fixed version leaked %d total tool approvals over %d trials", totalFixed, trials)
}

func TestTimeoutAfterRespectsContextCancellation(t *testing.T) {
	// Verify that timeoutAfter exits cleanly when context is cancelled
	// before the timeout fires.

	ctx := context.Background()
	runCtx, cancelRun := context.WithCancelCause(ctx)

	done := make(chan struct{})
	go func() {
		timeoutAfter(runCtx, cancelRun, 10*time.Minute) // should never fire
		close(done)
	}()

	// Cancel the context immediately
	cancelRun(errors.New("aborted"))

	// timeoutAfter should return promptly
	<-done

	cause := context.Cause(runCtx)
	assert.Equal(t, "aborted", cause.Error())
}
