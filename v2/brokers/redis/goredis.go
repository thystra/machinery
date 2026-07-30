package redis

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/RichardKnop/machinery/v2/brokers/errs"
	"github.com/RichardKnop/machinery/v2/brokers/iface"
	"github.com/RichardKnop/machinery/v2/common"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/log"
	"github.com/RichardKnop/machinery/v2/tasks"
)

const (
	defaultReliableClaimKeyPrefix          = "machinery:claims"
	defaultClaimLeaseDurationMilliseconds  = 120000
	defaultClaimRenewIntervalMilliseconds  = 30000
	defaultClaimRecoveryPeriodMilliseconds = 1000
	defaultClaimRecoveryBatchSize          = 100
)

var (
	claimNextTaskScript = redis.NewScript(`
local queue_type = redis.call("TYPE", KEYS[1])["ok"]
local payload_type = redis.call("TYPE", KEYS[2])["ok"]
local lease_type = redis.call("TYPE", KEYS[3])["ok"]
if queue_type ~= "none" and queue_type ~= "list" then
  return redis.error_reply("ready queue key is not a list")
end
if payload_type ~= "none" and payload_type ~= "hash" then
  return redis.error_reply("claim payload key is not a hash")
end
if lease_type ~= "none" and lease_type ~= "zset" then
  return redis.error_reply("claim lease key is not a sorted set")
end
local payload = redis.call("LPOP", KEYS[1])
if not payload then
  return false
end
redis.call("HSET", KEYS[2], ARGV[1], payload)
redis.call("ZADD", KEYS[3], ARGV[2], ARGV[1])
return payload
`)
	ackClaimScript = redis.NewScript(`
local payload_type = redis.call("TYPE", KEYS[1])["ok"]
local lease_type = redis.call("TYPE", KEYS[2])["ok"]
if payload_type ~= "none" and payload_type ~= "hash" then
  return redis.error_reply("claim payload key is not a hash")
end
if lease_type ~= "none" and lease_type ~= "zset" then
  return redis.error_reply("claim lease key is not a sorted set")
end
local removed = redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("HDEL", KEYS[1], ARGV[1])
return removed
`)
	renewClaimScript = redis.NewScript(`
local payload_type = redis.call("TYPE", KEYS[1])["ok"]
local lease_type = redis.call("TYPE", KEYS[2])["ok"]
if payload_type ~= "none" and payload_type ~= "hash" then
  return redis.error_reply("claim payload key is not a hash")
end
if lease_type ~= "none" and lease_type ~= "zset" then
  return redis.error_reply("claim lease key is not a sorted set")
end
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 0 then
  return 0
end
if redis.call("ZSCORE", KEYS[2], ARGV[1]) == false then
  return 0
end
redis.call("ZADD", KEYS[2], "XX", ARGV[2], ARGV[1])
return 1
`)
	releaseClaimScript = redis.NewScript(`
local queue_type = redis.call("TYPE", KEYS[1])["ok"]
local payload_type = redis.call("TYPE", KEYS[2])["ok"]
local lease_type = redis.call("TYPE", KEYS[3])["ok"]
if queue_type ~= "none" and queue_type ~= "list" then
  return redis.error_reply("ready queue key is not a list")
end
if payload_type ~= "none" and payload_type ~= "hash" then
  return redis.error_reply("claim payload key is not a hash")
end
if lease_type ~= "none" and lease_type ~= "zset" then
  return redis.error_reply("claim lease key is not a sorted set")
end
local payload = redis.call("HGET", KEYS[2], ARGV[1])
if payload then
  redis.call("RPUSH", KEYS[1], payload)
end
redis.call("HDEL", KEYS[2], ARGV[1])
redis.call("ZREM", KEYS[3], ARGV[1])
if payload then
  return 1
end
return 0
`)
	recoverExpiredClaimsScript = redis.NewScript(`
local queue_type = redis.call("TYPE", KEYS[1])["ok"]
local payload_type = redis.call("TYPE", KEYS[2])["ok"]
local lease_type = redis.call("TYPE", KEYS[3])["ok"]
if queue_type ~= "none" and queue_type ~= "list" then
  return redis.error_reply("ready queue key is not a list")
end
if payload_type ~= "none" and payload_type ~= "hash" then
  return redis.error_reply("claim payload key is not a hash")
end
if lease_type ~= "none" and lease_type ~= "zset" then
  return redis.error_reply("claim lease key is not a sorted set")
end
local claim_ids = redis.call(
  "ZRANGEBYSCORE",
  KEYS[3],
  "-inf",
  ARGV[1],
  "LIMIT",
  0,
  ARGV[2]
)
local recovered = 0
for _, claim_id in ipairs(claim_ids) do
  local payload = redis.call("HGET", KEYS[2], claim_id)
  if payload then
    redis.call("RPUSH", KEYS[1], payload)
    recovered = recovered + 1
  end
  redis.call("HDEL", KEYS[2], claim_id)
  redis.call("ZREM", KEYS[3], claim_id)
end
return recovered
`)
	promoteDelayedTaskScript = redis.NewScript(`
local delayed_type = redis.call("TYPE", KEYS[1])["ok"]
if delayed_type ~= "none" and delayed_type ~= "zset" then
  return redis.error_reply("delayed task key is not a sorted set")
end
local tasks = redis.call(
  "ZRANGEBYSCORE",
  KEYS[1],
  "-inf",
  ARGV[1],
  "LIMIT",
  0,
  1
)
if #tasks == 0 then
  return 0
end
local payload = tasks[1]
local ok, signature = pcall(cjson.decode, payload)
if not ok then
  return redis.error_reply("invalid delayed task JSON")
end
local queue = signature["RoutingKey"]
if not queue or queue == cjson.null or queue == "" then
  queue = ARGV[2]
end
local queue_type = redis.call("TYPE", queue)["ok"]
if queue_type ~= "none" and queue_type ~= "list" then
  return redis.error_reply("delayed task routing key is not a list")
end
redis.call("ZREM", KEYS[1], payload)
redis.call("RPUSH", queue, payload)
return 1
`)
)

type claimedDelivery struct {
	claimID         string
	queue           string
	payload         []byte
	renewalStop     chan struct{}
	renewalDone     chan struct{}
	renewalStopOnce sync.Once
}

// BrokerGR represents a Redis broker.
//
// The go-redis broker provides at-least-once delivery by atomically moving
// ready tasks into leased in-flight records before processing. A task is
// acknowledged only after TaskProcessor.Process returns nil. Expired claims
// are atomically returned to their original ready queue.
type BrokerGR struct {
	common.Broker
	rclient      redis.UniversalClient
	consumingWG  sync.WaitGroup // wait group to make sure whole consumption completes
	processingWG sync.WaitGroup // use wait group to make sure task processing completes
	delayedWG    sync.WaitGroup
	recoveryWG   sync.WaitGroup
	// If set, path to a socket file overrides hostname.
	socketPath           string
	redsync              *redsync.Redsync
	redisOnce            sync.Once
	redisDelayedTasksKey string
	reliableClaimsErr    error
}

// NewGR creates a new go-redis Broker instance.
func NewGR(cnf *config.Config, addrs []string, db int) iface.Broker {
	b := &BrokerGR{Broker: common.NewBroker(cnf)}

	var password string
	var username string
	parts := strings.Split(addrs[0], "@")
	if len(parts) >= 2 {
		// With password or username/password.
		options := strings.SplitN(strings.Join(parts[:len(parts)-1], "@"), ":", 2)
		if len(options) >= 2 {
			username = options[0]
			password = options[1]
		} else {
			password = options[0]
		}

		addrs[0] = parts[len(parts)-1]
	}

	ropt := &redis.UniversalOptions{
		Addrs:    addrs,
		DB:       db,
		Password: password,
		Username: username,
	}
	if cnf.Redis != nil {
		ropt.MasterName = cnf.Redis.MasterName
	}
	if cnf.TLSConfig != nil {
		ropt.TLSConfig = cnf.TLSConfig
	}

	if cnf.Redis != nil && cnf.Redis.SentinelPassword != "" {
		ropt.SentinelPassword = cnf.Redis.SentinelPassword
	}

	if cnf.Redis != nil && cnf.Redis.ClusterEnabled {
		b.rclient = redis.NewClusterClient(ropt.Cluster())
		b.reliableClaimsErr = fmt.Errorf(
			"Redis Cluster is not supported by the reliable go-redis broker: " +
				"ready, in-flight payload, and lease keys must share one atomic Redis context",
		)
	} else {
		b.rclient = redis.NewUniversalClient(ropt)
	}
	if cnf.Redis != nil && cnf.Redis.DelayedTasksKey != "" {
		b.redisDelayedTasksKey = cnf.Redis.DelayedTasksKey
	} else {
		b.redisDelayedTasksKey = defaultRedisDelayedTasksKey
	}
	return b
}

// StartConsuming enters a loop and waits for incoming messages.
func (b *BrokerGR) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	if b.reliableClaimsErr != nil {
		return false, b.reliableClaimsErr
	}

	b.consumingWG.Add(1)
	defer b.consumingWG.Done()

	if concurrency < 1 {
		concurrency = runtime.NumCPU() * 2
	}

	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)

	if _, err := b.rclient.Ping(context.Background()).Result(); err != nil {
		b.GetRetryFunc()(b.GetRetryStopChan())
		if b.GetRetry() {
			return b.GetRetry(), err
		}
		return b.GetRetry(), errs.ErrConsumerStopped
	}

	queue := getQueueGR(b.GetConfig(), taskProcessor)
	if recovered, err := b.recoverExpiredClaims(queue); err != nil {
		return b.GetRetry(), fmt.Errorf("recover expired Redis claims: %w", err)
	} else if recovered > 0 {
		log.WARNING.Printf("Recovered %d expired Redis task claim(s) for queue %s", recovered, queue)
	}

	deliveries := make(chan *claimedDelivery, concurrency)
	pool := make(chan struct{}, concurrency)
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}

	b.recoveryWG.Add(1)
	go b.runClaimRecovery(queue)

	go func() {
		log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

		for {
			select {
			case <-b.GetStopChan():
				close(deliveries)
				return
			case <-pool:
				claim, err := b.nextTask(queue, consumerTag)
				if err != nil && err != redis.Nil && err != errs.ErrConsumerStopped {
					log.ERROR.Printf("Claiming Redis task failed: %v", err)
				}
				if claim != nil {
					b.startClaimRenewal(claim)
					select {
					case deliveries <- claim:
					case <-b.GetStopChan():
						b.stopClaimRenewal(claim)
						if err := b.releaseClaim(claim); err != nil {
							log.ERROR.Printf("Releasing claimed Redis task during shutdown failed: %v", err)
						}
						pool <- struct{}{}
						close(deliveries)
						return
					}
				}
				pool <- struct{}{}
			}
		}
	}()

	b.delayedWG.Add(1)
	go func() {
		defer b.delayedWG.Done()
		for {
			select {
			case <-b.GetStopChan():
				return
			default:
				promoted, err := b.promoteNextDelayedTask()
				if err != nil && err != redis.Nil {
					log.ERROR.Printf("Promoting delayed Redis task failed: %v", err)
					continue
				}
				if promoted {
					log.DEBUG.Print("Promoted one due delayed Redis task atomically")
				}
			}
		}
	}()

	if err := b.consume(deliveries, concurrency, taskProcessor); err != nil {
		return b.GetRetry(), err
	}

	b.processingWG.Wait()
	return b.GetRetry(), nil
}

// StopConsuming quits the loop.
func (b *BrokerGR) StopConsuming() {
	b.Broker.StopConsuming()
	b.delayedWG.Wait()
	b.recoveryWG.Wait()
	b.consumingWG.Wait()
	b.rclient.Close()
}

// Publish places a new message on the default queue.
func (b *BrokerGR) Publish(ctx context.Context, signature *tasks.Signature) error {
	if b.reliableClaimsErr != nil {
		return b.reliableClaimsErr
	}

	b.Broker.AdjustRoutingKey(signature)

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	if signature.ETA != nil {
		now := time.Now().UTC()
		if signature.ETA.After(now) {
			score := signature.ETA.UnixNano()
			return b.rclient.ZAdd(ctx, b.redisDelayedTasksKey, redis.Z{
				Score:  float64(score),
				Member: msg,
			}).Err()
		}
	}

	return b.rclient.RPush(ctx, signature.RoutingKey, msg).Err()
}

// GetPendingTasks returns task signatures waiting in the ready queue.
func (b *BrokerGR) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	if queue == "" {
		queue = b.GetConfig().DefaultQueue
	}
	results, err := b.rclient.LRange(context.Background(), queue, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*tasks.Signature, len(results))
	for i, result := range results {
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(strings.NewReader(result))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, err
		}
		taskSignatures[i] = signature
	}
	return taskSignatures, nil
}

// GetDelayedTasks returns task signatures scheduled for future promotion.
func (b *BrokerGR) GetDelayedTasks() ([]*tasks.Signature, error) {
	results, err := b.rclient.ZRange(context.Background(), b.redisDelayedTasksKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*tasks.Signature, len(results))
	for i, result := range results {
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(strings.NewReader(result))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, err
		}
		taskSignatures[i] = signature
	}
	return taskSignatures, nil
}

// consume takes claimed messages from the channel and manages a worker pool.
func (b *BrokerGR) consume(deliveries <-chan *claimedDelivery, concurrency int, taskProcessor iface.TaskProcessor) error {
	errorsChan := make(chan error, concurrency*2)
	pool := make(chan struct{}, concurrency)

	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	for {
		select {
		case err := <-errorsChan:
			return err
		case claim, open := <-deliveries:
			if !open {
				return nil
			}
			if concurrency > 0 {
				<-pool
			}

			b.processingWG.Add(1)
			go func(delivery *claimedDelivery) {
				defer b.processingWG.Done()
				defer func() {
					if concurrency > 0 {
						pool <- struct{}{}
					}
				}()

				if err := b.consumeOne(delivery, taskProcessor); err != nil {
					errorsChan <- err
				}
			}(claim)
		}
	}
}

// consumeOne processes one leased message and acknowledges it only after the
// TaskProcessor reports durable completion by returning nil.
func (b *BrokerGR) consumeOne(delivery *claimedDelivery, taskProcessor iface.TaskProcessor) error {
	defer b.stopClaimRenewal(delivery)

	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(delivery.payload))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		decodeErr := errs.NewErrCouldNotUnmarshalTaskSignature(delivery.payload, err)
		if ackErr := b.ackClaim(delivery); ackErr != nil {
			return fmt.Errorf("%v; acknowledging malformed task failed: %w", decodeErr, ackErr)
		}
		return decodeErr
	}

	if !b.IsTaskRegistered(signature.Name) {
		if signature.IgnoreWhenTaskNotRegistered {
			return b.ackClaim(delivery)
		}

		log.INFO.Printf("Task not registered with this worker. Requeuing message: %s", delivery.payload)
		return b.releaseClaim(delivery)
	}

	log.DEBUG.Printf("Received new message: %s", delivery.payload)
	if err := taskProcessor.Process(signature); err != nil {
		return err
	}

	return b.ackClaim(delivery)
}

func (b *BrokerGR) nextTask(queue, consumerTag string) (*claimedDelivery, error) {
	claim, err := b.claimNextTask(queue, consumerTag)
	if err == redis.Nil {
		pollPeriod := b.normalTaskPollPeriod()
		select {
		case <-b.GetStopChan():
			return nil, errs.ErrConsumerStopped
		case <-time.After(pollPeriod):
			return nil, redis.Nil
		}
	}
	return claim, err
}

func (b *BrokerGR) claimNextTask(queue, consumerTag string) (*claimedDelivery, error) {
	payloadKey, leaseKey := b.claimKeys(queue)
	claimID := fmt.Sprintf("%s:%s", consumerTag, uuid.NewString())
	leaseDeadline := time.Now().Add(b.claimLeaseDuration()).UnixMilli()

	payloadText, err := claimNextTaskScript.Run(
		context.Background(),
		b.rclient,
		[]string{queue, payloadKey, leaseKey},
		claimID,
		leaseDeadline,
	).Text()
	if err != nil {
		return nil, err
	}

	return &claimedDelivery{
		claimID:     claimID,
		queue:       queue,
		payload:     []byte(payloadText),
		renewalStop: make(chan struct{}),
		renewalDone: make(chan struct{}),
	}, nil
}

func (b *BrokerGR) startClaimRenewal(delivery *claimedDelivery) {
	go func() {
		defer close(delivery.renewalDone)
		ticker := time.NewTicker(b.claimRenewInterval())
		defer ticker.Stop()

		for {
			select {
			case <-delivery.renewalStop:
				return
			case <-ticker.C:
				if renewed, err := b.renewClaim(delivery); err != nil {
					log.ERROR.Printf("Renewing Redis task claim %s failed: %v", delivery.claimID, err)
				} else if !renewed {
					log.WARNING.Printf("Redis task claim %s no longer exists while processing", delivery.claimID)
					return
				}
			}
		}
	}()
}

func (b *BrokerGR) stopClaimRenewal(delivery *claimedDelivery) {
	delivery.renewalStopOnce.Do(func() {
		close(delivery.renewalStop)
	})
	<-delivery.renewalDone
}

func (b *BrokerGR) renewClaim(delivery *claimedDelivery) (bool, error) {
	payloadKey, leaseKey := b.claimKeys(delivery.queue)
	leaseDeadline := time.Now().Add(b.claimLeaseDuration()).UnixMilli()
	updated, err := renewClaimScript.Run(
		context.Background(),
		b.rclient,
		[]string{payloadKey, leaseKey},
		delivery.claimID,
		leaseDeadline,
	).Int64()
	return updated == 1, err
}

func (b *BrokerGR) ackClaim(delivery *claimedDelivery) error {
	payloadKey, leaseKey := b.claimKeys(delivery.queue)
	_, err := ackClaimScript.Run(
		context.Background(),
		b.rclient,
		[]string{payloadKey, leaseKey},
		delivery.claimID,
	).Int64()
	return err
}

func (b *BrokerGR) releaseClaim(delivery *claimedDelivery) error {
	payloadKey, leaseKey := b.claimKeys(delivery.queue)
	_, err := releaseClaimScript.Run(
		context.Background(),
		b.rclient,
		[]string{delivery.queue, payloadKey, leaseKey},
		delivery.claimID,
	).Int64()
	return err
}

func (b *BrokerGR) recoverExpiredClaims(queue string) (int64, error) {
	payloadKey, leaseKey := b.claimKeys(queue)
	return recoverExpiredClaimsScript.Run(
		context.Background(),
		b.rclient,
		[]string{queue, payloadKey, leaseKey},
		time.Now().UnixMilli(),
		b.claimRecoveryBatchSize(),
	).Int64()
}

func (b *BrokerGR) runClaimRecovery(queue string) {
	defer b.recoveryWG.Done()
	ticker := time.NewTicker(b.claimRecoveryPeriod())
	defer ticker.Stop()

	for {
		select {
		case <-b.GetStopChan():
			return
		case <-ticker.C:
			recovered, err := b.recoverExpiredClaims(queue)
			if err != nil {
				log.ERROR.Printf("Recovering expired Redis task claims failed: %v", err)
				continue
			}
			if recovered > 0 {
				log.WARNING.Printf("Recovered %d expired Redis task claim(s) for queue %s", recovered, queue)
			}
		}
	}
}

func (b *BrokerGR) promoteNextDelayedTask() (bool, error) {
	pollPeriod := b.delayedTaskPollPeriod()
	select {
	case <-b.GetStopChan():
		return false, errs.ErrConsumerStopped
	case <-time.After(pollPeriod):
	}

	promoted, err := promoteDelayedTaskScript.Run(
		context.Background(),
		b.rclient,
		[]string{b.redisDelayedTasksKey},
		time.Now().UTC().UnixNano(),
		b.GetConfig().DefaultQueue,
	).Int64()
	return promoted == 1, err
}

func (b *BrokerGR) claimKeys(queue string) (string, string) {
	encodedQueue := base64.RawURLEncoding.EncodeToString([]byte(queue))
	prefix := defaultReliableClaimKeyPrefix
	if redisConfig := b.GetConfig().Redis; redisConfig != nil && redisConfig.ClaimKeyPrefix != "" {
		prefix = redisConfig.ClaimKeyPrefix
	}
	return fmt.Sprintf("%s:%s:payloads", prefix, encodedQueue),
		fmt.Sprintf("%s:%s:leases", prefix, encodedQueue)
}

func (b *BrokerGR) normalTaskPollPeriod() time.Duration {
	milliseconds := 1000
	if redisConfig := b.GetConfig().Redis; redisConfig != nil && redisConfig.NormalTasksPollPeriod > 0 {
		milliseconds = redisConfig.NormalTasksPollPeriod
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func (b *BrokerGR) delayedTaskPollPeriod() time.Duration {
	milliseconds := 500
	if redisConfig := b.GetConfig().Redis; redisConfig != nil && redisConfig.DelayedTasksPollPeriod > 0 {
		milliseconds = redisConfig.DelayedTasksPollPeriod
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func (b *BrokerGR) claimLeaseDuration() time.Duration {
	milliseconds := defaultClaimLeaseDurationMilliseconds
	if redisConfig := b.GetConfig().Redis; redisConfig != nil && redisConfig.ClaimLeaseDuration > 0 {
		milliseconds = redisConfig.ClaimLeaseDuration
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func (b *BrokerGR) claimRenewInterval() time.Duration {
	milliseconds := defaultClaimRenewIntervalMilliseconds
	if redisConfig := b.GetConfig().Redis; redisConfig != nil && redisConfig.ClaimRenewInterval > 0 {
		milliseconds = redisConfig.ClaimRenewInterval
	}
	lease := b.claimLeaseDuration()
	interval := time.Duration(milliseconds) * time.Millisecond
	if interval >= lease {
		return lease / 3
	}
	return interval
}

func (b *BrokerGR) claimRecoveryPeriod() time.Duration {
	milliseconds := defaultClaimRecoveryPeriodMilliseconds
	if redisConfig := b.GetConfig().Redis; redisConfig != nil && redisConfig.ClaimRecoveryPeriod > 0 {
		milliseconds = redisConfig.ClaimRecoveryPeriod
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func (b *BrokerGR) claimRecoveryBatchSize() int {
	batchSize := defaultClaimRecoveryBatchSize
	if redisConfig := b.GetConfig().Redis; redisConfig != nil && redisConfig.ClaimRecoveryBatchSize > 0 {
		batchSize = redisConfig.ClaimRecoveryBatchSize
	}
	return batchSize
}

func getQueueGR(config *config.Config, taskProcessor iface.TaskProcessor) string {
	customQueue := taskProcessor.CustomQueue()
	if customQueue == "" {
		return config.DefaultQueue
	}
	return customQueue
}
