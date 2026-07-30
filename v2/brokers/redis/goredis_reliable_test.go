package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/tasks"
)

type reliableClaimsTestProcessor struct {
	queue string
	err   error
}

func (p *reliableClaimsTestProcessor) Process(_ *tasks.Signature) error {
	return p.err
}

func (p *reliableClaimsTestProcessor) CustomQueue() string {
	return p.queue
}

func (p *reliableClaimsTestProcessor) PreConsumeHandler() bool {
	return true
}

func newReliableClaimsTestBroker(t *testing.T) (*BrokerGR, string) {
	t.Helper()

	address := os.Getenv("REDIS_URL")
	if address == "" {
		t.Skip("REDIS_URL is not set")
	}
	address = strings.TrimPrefix(address, "redis://")

	queue := "machinery_reliable_claims_test_" + uuid.NewString()
	cnf := &config.Config{
		DefaultQueue: queue,
		Redis: &config.RedisConfig{
			NormalTasksPollPeriod:  5,
			DelayedTasksPollPeriod: 5,
			DelayedTasksKey:        queue + ":delayed",
			ClaimLeaseDuration:     500,
			ClaimRenewInterval:     50,
			ClaimRecoveryPeriod:    10,
			ClaimRecoveryBatchSize: 100,
			ClaimKeyPrefix:         queue + ":claims",
		},
	}

	broker, ok := NewGR(cnf, []string{address}, 0).(*BrokerGR)
	require.True(t, ok)
	require.NoError(t, broker.rclient.Ping(context.Background()).Err())

	t.Cleanup(func() {
		payloadKey, leaseKey := broker.claimKeys(queue)
		_ = broker.rclient.Del(
			context.Background(),
			queue,
			payloadKey,
			leaseKey,
			cnf.Redis.DelayedTasksKey,
		).Err()
		_ = broker.rclient.Close()
	})

	return broker, queue
}

func reliableClaimsTestPayload(t *testing.T, queue string) []byte {
	t.Helper()

	payload, err := json.Marshal(&tasks.Signature{
		UUID:       "task_" + uuid.NewString(),
		Name:       "reliable-claims-test",
		RoutingKey: queue,
	})
	require.NoError(t, err)
	return payload
}

func TestReliableClaimAcknowledgementBoundary(t *testing.T) {
	broker, queue := newReliableClaimsTestBroker(t)
	broker.SetRegisteredTaskNames([]string{"reliable-claims-test"})

	processor := &reliableClaimsTestProcessor{queue: queue}
	payload := reliableClaimsTestPayload(t, queue)
	require.NoError(t, broker.rclient.RPush(context.Background(), queue, payload).Err())

	claim, err := broker.claimNextTask(queue, "ack-test")
	require.NoError(t, err)
	broker.startClaimRenewal(claim)
	require.NoError(t, broker.consumeOne(claim, processor))

	payloadKey, leaseKey := broker.claimKeys(queue)
	require.EqualValues(t, 0, broker.rclient.HLen(context.Background(), payloadKey).Val())
	require.EqualValues(t, 0, broker.rclient.ZCard(context.Background(), leaseKey).Val())

	processor.err = errors.New("processor infrastructure failure")
	payload = reliableClaimsTestPayload(t, queue)
	require.NoError(t, broker.rclient.RPush(context.Background(), queue, payload).Err())

	claim, err = broker.claimNextTask(queue, "failure-test")
	require.NoError(t, err)
	broker.startClaimRenewal(claim)
	require.EqualError(t, broker.consumeOne(claim, processor), "processor infrastructure failure")

	require.EqualValues(t, 1, broker.rclient.HLen(context.Background(), payloadKey).Val())
	require.EqualValues(t, 1, broker.rclient.ZCard(context.Background(), leaseKey).Val())
	require.NoError(t, broker.releaseClaim(claim))
}

func TestReliableClaimsKeepIdenticalPayloadsDistinct(t *testing.T) {
	broker, queue := newReliableClaimsTestBroker(t)
	payload := reliableClaimsTestPayload(t, queue)

	require.NoError(t, broker.rclient.RPush(context.Background(), queue, payload, payload).Err())

	first, err := broker.claimNextTask(queue, "same-consumer")
	require.NoError(t, err)
	second, err := broker.claimNextTask(queue, "same-consumer")
	require.NoError(t, err)

	require.NotEqual(t, first.claimID, second.claimID)
	require.Equal(t, first.payload, second.payload)

	payloadKey, leaseKey := broker.claimKeys(queue)
	require.EqualValues(t, 2, broker.rclient.HLen(context.Background(), payloadKey).Val())
	require.EqualValues(t, 2, broker.rclient.ZCard(context.Background(), leaseKey).Val())

	require.NoError(t, broker.ackClaim(first))
	require.NoError(t, broker.ackClaim(second))
}

func TestExpiredClaimRecoveryIsAtomic(t *testing.T) {
	broker, queue := newReliableClaimsTestBroker(t)
	payload := reliableClaimsTestPayload(t, queue)
	require.NoError(t, broker.rclient.RPush(context.Background(), queue, payload).Err())

	claim, err := broker.claimNextTask(queue, "expired-test")
	require.NoError(t, err)
	payloadKey, leaseKey := broker.claimKeys(queue)
	require.NoError(t, broker.rclient.ZAdd(
		context.Background(),
		leaseKey,
		goredis.Z{Score: float64(time.Now().Add(-time.Second).UnixMilli()), Member: claim.claimID},
	).Err())

	recovered, err := broker.recoverExpiredClaims(queue)
	require.NoError(t, err)
	require.EqualValues(t, 1, recovered)
	require.EqualValues(t, 1, broker.rclient.LLen(context.Background(), queue).Val())
	require.EqualValues(t, 0, broker.rclient.HLen(context.Background(), payloadKey).Val())
	require.EqualValues(t, 0, broker.rclient.ZCard(context.Background(), leaseKey).Val())
}

func TestRenewClaimReportsExistingLeaseUpdate(t *testing.T) {
	broker, queue := newReliableClaimsTestBroker(t)
	payload := reliableClaimsTestPayload(t, queue)
	require.NoError(t, broker.rclient.RPush(context.Background(), queue, payload).Err())

	claim, err := broker.claimNextTask(queue, "renew-return-test")
	require.NoError(t, err)
	payloadKey, leaseKey := broker.claimKeys(queue)
	before := broker.rclient.ZScore(context.Background(), leaseKey, claim.claimID).Val()

	time.Sleep(2 * time.Millisecond)
	renewed, err := broker.renewClaim(claim)
	require.NoError(t, err)
	require.True(t, renewed)
	after := broker.rclient.ZScore(context.Background(), leaseKey, claim.claimID).Val()
	require.Greater(t, after, before)
	require.True(t, broker.rclient.HExists(context.Background(), payloadKey, claim.claimID).Val())
	require.EqualValues(t, 1, broker.rclient.ZCard(context.Background(), leaseKey).Val())

	require.NoError(t, broker.ackClaim(claim))
}

func TestClaimRenewalPreventsPrematureRecovery(t *testing.T) {
	broker, queue := newReliableClaimsTestBroker(t)
	payload := reliableClaimsTestPayload(t, queue)
	require.NoError(t, broker.rclient.RPush(context.Background(), queue, payload).Err())

	claim, err := broker.claimNextTask(queue, "renewal-test")
	require.NoError(t, err)
	broker.startClaimRenewal(claim)

	time.Sleep(750 * time.Millisecond)
	recovered, err := broker.recoverExpiredClaims(queue)
	require.NoError(t, err)
	require.EqualValues(t, 0, recovered)
	require.EqualValues(t, 0, broker.rclient.LLen(context.Background(), queue).Val())

	broker.stopClaimRenewal(claim)
	require.NoError(t, broker.ackClaim(claim))
}

func TestClaimRenewalContinuesWhileBrokerIsStopping(t *testing.T) {
	broker, queue := newReliableClaimsTestBroker(t)
	payload := reliableClaimsTestPayload(t, queue)
	require.NoError(t, broker.rclient.RPush(context.Background(), queue, payload).Err())

	claim, err := broker.claimNextTask(queue, "graceful-stop-renewal-test")
	require.NoError(t, err)
	broker.startClaimRenewal(claim)
	close(broker.GetStopChan())

	time.Sleep(750 * time.Millisecond)
	recovered, err := broker.recoverExpiredClaims(queue)
	require.NoError(t, err)
	require.EqualValues(t, 0, recovered)

	broker.stopClaimRenewal(claim)
	require.NoError(t, broker.ackClaim(claim))
}

func TestReleaseClaimAtomicallyRequeuesTask(t *testing.T) {
	broker, queue := newReliableClaimsTestBroker(t)
	payload := reliableClaimsTestPayload(t, queue)
	require.NoError(t, broker.rclient.RPush(context.Background(), queue, payload).Err())

	claim, err := broker.claimNextTask(queue, "release-test")
	require.NoError(t, err)
	require.NoError(t, broker.releaseClaim(claim))

	payloadKey, leaseKey := broker.claimKeys(queue)
	require.EqualValues(t, 1, broker.rclient.LLen(context.Background(), queue).Val())
	require.EqualValues(t, 0, broker.rclient.HLen(context.Background(), payloadKey).Val())
	require.EqualValues(t, 0, broker.rclient.ZCard(context.Background(), leaseKey).Val())
}

func TestDelayedTaskPromotionIsAtomic(t *testing.T) {
	broker, queue := newReliableClaimsTestBroker(t)
	targetQueue := queue + ":target"
	t.Cleanup(func() {
		_ = broker.rclient.Del(context.Background(), targetQueue).Err()
	})

	payload, err := json.Marshal(&tasks.Signature{
		UUID:       "task_" + uuid.NewString(),
		Name:       "reliable-claims-test",
		RoutingKey: targetQueue,
	})
	require.NoError(t, err)
	require.NoError(t, broker.rclient.ZAdd(
		context.Background(),
		broker.redisDelayedTasksKey,
		goredis.Z{Score: float64(time.Now().Add(-time.Second).UnixNano()), Member: payload},
	).Err())

	promoted, err := broker.promoteNextDelayedTask()
	require.NoError(t, err)
	require.True(t, promoted)
	require.EqualValues(t, 0, broker.rclient.ZCard(context.Background(), broker.redisDelayedTasksKey).Val())
	require.EqualValues(t, 1, broker.rclient.LLen(context.Background(), targetQueue).Val())
}

func TestMalformedDelayedTaskRemainsForDiagnosis(t *testing.T) {
	broker, _ := newReliableClaimsTestBroker(t)
	malformed := "not-json"
	require.NoError(t, broker.rclient.ZAdd(
		context.Background(),
		broker.redisDelayedTasksKey,
		goredis.Z{Score: float64(time.Now().Add(-time.Second).UnixNano()), Member: malformed},
	).Err())

	promoted, err := broker.promoteNextDelayedTask()
	require.Error(t, err)
	require.False(t, promoted)
	require.EqualValues(t, 1, broker.rclient.ZCard(context.Background(), broker.redisDelayedTasksKey).Val())
}

func TestReliableClaimsRejectRedisCluster(t *testing.T) {
	cnf := &config.Config{
		DefaultQueue: "cluster-test",
		Redis: &config.RedisConfig{
			ClusterEnabled: true,
		},
	}

	broker, ok := NewGR(cnf, []string{"127.0.0.1:6379"}, 0).(*BrokerGR)
	require.True(t, ok)
	t.Cleanup(func() { _ = broker.rclient.Close() })

	err := broker.Publish(context.Background(), &tasks.Signature{
		UUID: "cluster-test",
		Name: "test",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Redis Cluster is not supported")
}

func TestClaimKeysAreStableAndQueueSpecific(t *testing.T) {
	broker, queue := newReliableClaimsTestBroker(t)
	firstPayload, firstLease := broker.claimKeys(queue)
	secondPayload, secondLease := broker.claimKeys(queue)
	otherPayload, otherLease := broker.claimKeys(queue + ":other")

	require.Equal(t, firstPayload, secondPayload)
	require.Equal(t, firstLease, secondLease)
	require.NotEqual(t, firstPayload, otherPayload)
	require.NotEqual(t, firstLease, otherLease)
	require.Contains(t, firstPayload, fmt.Sprintf("%s:claims", queue))
}
