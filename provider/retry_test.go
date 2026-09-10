package provider

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestRetryOnConcurrentTableUpdateRetriesWhileTableBusy(t *testing.T) {
	for _, code := range []string{
		dynamodb.ErrCodeResourceInUseException,
		dynamodb.ErrCodeLimitExceededException,
	} {
		t.Run(code, func(t *testing.T) {
			calls := 0
			err := retryOnConcurrentTableUpdate(30*time.Second, func() error {
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

func TestRetryOnConcurrentTableUpdateSurfacesOtherErrors(t *testing.T) {
	calls := 0
	sentinel := awserr.New(dynamodb.ErrCodeResourceNotFoundException, "no such table", nil)

	err := retryOnConcurrentTableUpdate(30*time.Second, func() error {
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

func TestRetryOnConcurrentTableUpdateGivesUpAtTimeout(t *testing.T) {
	busy := awserr.New(dynamodb.ErrCodeResourceInUseException, "table is being updated", nil)

	err := retryOnConcurrentTableUpdate(time.Second, func() error {
		return busy
	})

	if err == nil {
		t.Fatal("expected a timeout error when the table never frees up")
	}
	if !errors.Is(err, busy) {
		t.Fatalf("expected the last AWS error to be surfaced, got %v", err)
	}
}

func TestRetryOnConcurrentTableUpdatePassesThroughSuccess(t *testing.T) {
	calls := 0

	if err := retryOnConcurrentTableUpdate(30*time.Second, func() error {
		calls++
		return nil
	}); err != nil {
		t.Fatalf("expected success, got %v", err)
	}

	if calls != 1 {
		t.Fatalf("expected exactly 1 call, got %d", calls)
	}
}
