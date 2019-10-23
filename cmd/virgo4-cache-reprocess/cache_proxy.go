package main

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
	"log"
	"time"
	//"log"
)

// the number of keys to lookup at once
var lookupCacheMaxKeyCount = 200

// log a warning if any Redis request takes longer than this
var warnIfRequestTakesLonger = int64(100)

type CacheProxy interface {
	Exists([]string) (bool, error)
	Get([]string) ([]awssqs.Message, error)
}

// our implementation
type cacheProxyImpl struct {
	redis *redis.Client
}

//
// factory
//
func NewCacheProxy(config *ServiceConfig) (CacheProxy, error) {

	impl := &cacheProxyImpl{}

	options := &redis.Options{
		DialTimeout: time.Duration(config.RedisTimeout) * time.Second,
		ReadTimeout: time.Duration(config.RedisTimeout) * time.Second,
		Addr:        fmt.Sprintf("%s:%d", config.RedisHost, config.RedisPort),
		Password:    config.RedisPass,
		DB:          config.RedisDB,
		PoolSize:    config.Workers,
	}
	impl.redis = redis.NewClient(options)

	_, err := impl.redis.Ping().Result()
	return impl, err
}

//
// do all of the supplied keys exist in the cache
//
func (ci *cacheProxyImpl) Exists(keys []string) (bool, error) {

	// lookup the id in the cache
	start := time.Now()
	r, err := ci.redis.Exists(keys...).Result()
	elapsed := int64(time.Since(start) / time.Millisecond)
	// we want to warn if the request took a long time
	ci.warnIfSlow(elapsed, fmt.Sprintf("redis Exists (%d items)", len(keys)))

	if err != nil {
		return false, err
	}

	//log.Printf("lookup %d keys in %0.2f seconds", len( keys ), duration.Seconds())

	if r == 0 {
		log.Printf("ERROR: one or more keys do not exist in the cache")
	}
	//log.Printf("ID %s: result %d, error %t", key, r, err)
	return r != 0, err
}

//
// get the specified items from the cache
//
func (ci *cacheProxyImpl) Get(keys []string) ([]awssqs.Message, error) {

	// the response
	messages := make([]awssqs.Message, 0, len(keys))

	// specify the field list
	fields := []string{"type", "source", "payload"}

	// create the command pipeline
	cmdPipeline := ci.redis.Pipeline()
	m := map[string]*redis.SliceCmd{}
	for _, id := range keys {
		m[id] = cmdPipeline.HMGet(id, fields...)
	}

	start := time.Now()
	_, err := cmdPipeline.Exec()
	elapsed := int64(time.Since(start) / time.Millisecond)
	ci.warnIfSlow(elapsed, fmt.Sprintf("redis HMGet (%d items)", len(keys)))
	if err != nil {
		return nil, err
	}

	// go through the command responses
	for id, v := range m {

		res, err := v.Result()
		if err != nil {
			log.Printf("WARNING: one of the cache operations failed, ignoring lookup")
			continue
		}

		// these are the fields in the order we requested them in

		if res[0] == nil {
			log.Printf("ERROR: cache type value is empty, ignoring lookup")
			continue
		}

		if res[1] == nil {
			log.Printf("ERROR: cache source value is empty, ignoring lookup")
			continue
		}

		if res[2] == nil {
			log.Printf("ERROR: cache payload value is empty, ignoring lookup")
			continue
		}

		// extract the field values
		t := fmt.Sprintf("%s", res[0])
		s := fmt.Sprintf("%s", res[1])
		p := fmt.Sprintf("%s", res[2])
		messages = append(messages, ci.constructMessage(id, t, s, p))
	}

	return messages, nil
}

func (ci *cacheProxyImpl) constructMessage(id string, theType string, source string, payload string) awssqs.Message {

	attributes := make([]awssqs.Attribute, 0, 4)
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordId, Value: id})
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordType, Value: theType})
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordSource, Value: source})
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordOperation, Value: awssqs.AttributeValueRecordOperationUpdate})
	return awssqs.Message{Attribs: attributes, Payload: []byte(payload)}
}

// sometimes it is interesting to know if our SQS queries are slow
func (ci *cacheProxyImpl) warnIfSlow(elapsed int64, prefix string) {

	if elapsed >= warnIfRequestTakesLonger {
		log.Printf("INFO: %s elapsed %d ms", prefix, elapsed)
	}
}

//
// end of file
//
