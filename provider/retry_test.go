package provider

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// shortenBackoff keeps the retry tests fast; they assert on call counts and
// ordering, not on the real production wait.
func shortenBackoff(t *testing.T) {
	t.Helper()

	initial, max := initialRetryBackoff, maxRetryBackoff
	initialRetryBackoff, maxRetryBackoff = 2*time.Millisecond, 10*time.Millisecond

	t.Cleanup(func() {
		initialRetryBackoff, maxRetryBackoff = initial, max
	})
}

func busyErr() error {
	return awserr.New(dynamodb.ErrCodeResourceInUseException, "table is being updated", nil)
}

func TestRetryOnConcurrentTableUpdateRetriesWhileTableBusy(t *testing.T) {
	shortenBackoff(t)

	for _, code := range []string{
		dynamodb.ErrCodeResourceInUseException,
		dynamodb.ErrCodeLimitExceededException,
	} {
		t.Run(code, func(t *testing.T) {
			calls := 0
			err := retryOnConcurrentTableUpdate(time.Minute, func() error {
				calls++
				if calls < 3 {
					return awserr.New(code, "table is being updated", nil)
				}
				return nil
			})

			if err != nil {
				t.Fatalf("expected eventual success, got %v", err)
			}
			if calls != 3 {
				t.Fatalf("expected 3 calls, got %d", calls)
			}
		})
	}
}

func TestRetryOnConcurrentTableUpdateCapsRetries(t *testing.T) {
	shortenBackoff(t)

	calls := 0
	err := retryOnConcurrentTableUpdate(time.Minute, func() error {
		calls++
		return busyErr()
	})

	// One initial attempt plus maxTableUpdateRetries retries.
	if want := maxTableUpdateRetries + 1; calls != want {
		t.Fatalf("expected %d calls, got %d", want, calls)
	}
	if !isTableBusy(err) {
		t.Fatalf("expected a table-busy error, got %v", err)
	}
}

func TestRetryOnConcurrentTableUpdateSurfacesOtherErrors(t *testing.T) {
	shortenBackoff(t)

	calls := 0
	sentinel := awserr.New(dynamodb.ErrCodeResourceNotFoundException, "no such table", nil)

	err := retryOnConcurrentTableUpdate(time.Minute, func() error {
		calls++
		return sentinel
	})

	if calls != 1 {
		t.Fatalf("expected no retry on a non-retryable error, got %d calls", calls)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected the original error to be surfaced, got %v", err)
	}

	// dynamoDBGSIDelete type-asserts directly rather than using errors.As, so
	// the returned error must stay an awserr.Error for its not-found branch.
	aerr, ok := err.(awserr.Error)
	if !ok {
		t.Fatalf("expected an awserr.Error to survive the retry wrapper, got %T", err)
	}
	if aerr.Code() != dynamodb.ErrCodeResourceNotFoundException {
		t.Fatalf("expected the original error code, got %s", aerr.Code())
	}
}

func TestRetryOnConcurrentTableUpdateStopsAtDeadline(t *testing.T) {
	calls := 0

	// The real backoff is longer than the timeout, so there is no point
	// sleeping: the call should give up after the first attempt.
	start := time.Now()
	err := retryOnConcurrentTableUpdate(time.Millisecond, func() error {
		calls++
		return busyErr()
	})

	if calls != 1 {
		t.Fatalf("expected to give up without sleeping past the deadline, got %d calls", calls)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("expected an immediate return, took %s", elapsed)
	}
	if !isTableBusy(err) {
		t.Fatalf("expected the last busy error, got %v", err)
	}
}

func TestRetryOnConcurrentTableUpdatePassesThroughSuccess(t *testing.T) {
	calls := 0

	if err := retryOnConcurrentTableUpdate(time.Minute, func() error {
		calls++
		return nil
	}); err != nil {
		t.Fatalf("expected success, got %v", err)
	}

	if calls != 1 {
		t.Fatalf("expected exactly 1 call, got %d", calls)
	}
}

func TestJitteredBackoffStaysInRangeAndVaries(t *testing.T) {
	seen := map[time.Duration]bool{}

	for i := 0; i < 50; i++ {
		got := jitteredBackoff(1)
		if got < initialRetryBackoff/2 || got >= initialRetryBackoff {
			t.Fatalf("backoff %s outside [%s, %s)", got, initialRetryBackoff/2, initialRetryBackoff)
		}
		seen[got] = true
	}

	if len(seen) == 1 {
		t.Fatal("expected jitter to produce varying backoffs")
	}
}

func TestJitteredBackoffGrowsAndCaps(t *testing.T) {
	// Attempt 1 draws from [1s, 2s), attempt 2 from [2s, 4s): the ranges do
	// not overlap, so growth is observable without flaking on the jitter.
	if first, second := jitteredBackoff(1), jitteredBackoff(2); second <= first {
		t.Fatalf("expected backoff to grow, got %s then %s", first, second)
	}

	if got := jitteredBackoff(20); got >= maxRetryBackoff {
		t.Fatalf("expected backoff to cap below %s, got %s", maxRetryBackoff, got)
	}
}

func TestLockTableSerializesSameTable(t *testing.T) {
	const goroutines = 8

	var (
		inFlight int32
		overlaps int32
		wg       sync.WaitGroup
	)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			unlock, err := lockTable("serialized-table", time.Minute)
			if err != nil {
				t.Errorf("unexpected lock error: %v", err)
				return
			}
			defer unlock()

			if atomic.AddInt32(&inFlight, 1) > 1 {
				atomic.AddInt32(&overlaps, 1)
			}
			time.Sleep(time.Millisecond)
			atomic.AddInt32(&inFlight, -1)
		}()
	}

	wg.Wait()

	if overlaps != 0 {
		t.Fatalf("expected index operations on one table never to overlap, saw %d overlaps", overlaps)
	}
}

func TestLockTableDoesNotBlockOtherTables(t *testing.T) {
	unlock, err := lockTable("table-a", time.Minute)
	if err != nil {
		t.Fatalf("unexpected lock error: %v", err)
	}
	defer unlock()

	// A different table must not be gated on table-a's in-flight operation.
	otherUnlock, err := lockTable("table-b", 5*time.Second)
	if err != nil {
		t.Fatalf("expected an unrelated table to lock freely, got %v", err)
	}
	otherUnlock()
}

func TestLockTableTimesOutNamingTheTable(t *testing.T) {
	unlock, err := lockTable("busy-table", time.Minute)
	if err != nil {
		t.Fatalf("unexpected lock error: %v", err)
	}
	defer unlock()

	_, err = lockTable("busy-table", 10*time.Millisecond)
	if err == nil {
		t.Fatal("expected a timeout while the table was held")
	}
	if !strings.Contains(err.Error(), "busy-table") {
		t.Fatalf("expected the error to name the table, got %q", err)
	}
}

func TestLockTableReleasesOnUnlock(t *testing.T) {
	tn := fmt.Sprintf("released-table-%d", time.Now().UnixNano())

	unlock, err := lockTable(tn, time.Minute)
	if err != nil {
		t.Fatalf("unexpected lock error: %v", err)
	}
	unlock()

	second, err := lockTable(tn, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("expected the lock to be reacquirable after unlock, got %v", err)
	}
	second()
}
