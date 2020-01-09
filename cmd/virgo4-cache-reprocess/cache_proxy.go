package main

import (
	"fmt"
	"log"
	"time"

	dbx "github.com/go-ozzo/ozzo-dbx"
	_ "github.com/lib/pq"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

// errors returned by the cache
var ErrNotInCache = fmt.Errorf("item(s) not in cache")

// the maximum number of keys to lookup at once
var lookupCacheMaxKeyCount = 500

// the maximum number of keys to get at once
var getCacheMaxKeyCount = 100

// log a warning if any postgres request takes longer than this
var lookupRequestTimeLimit = int64(300)
var getRequestTimeLimit = int64(300)

type CacheProxy interface {
	Exists([]string) (bool, error)
	Get([]string) ([]awssqs.Message, error)
}

// our implementation
type cacheProxyImpl struct {
	tableName string
	db        *dbx.DB
}

//
// factory
//
func NewCacheProxy(config *ServiceConfig) (CacheProxy, error) {

	impl := &cacheProxyImpl{}

	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%d connect_timeout=%d sslmode=disable",
		config.PostgresUser, config.PostgresPass, config.PostgresDatabase, config.PostgresHost, config.PostgresPort, 30)

	db, err := dbx.MustOpen("postgres", connStr)
	if err != nil {
		return nil, err
	}

	// uncomment for SQL logging
	//db.LogFunc = log.Printf

	impl.tableName = config.PostgresTable
	impl.db = db
	return impl, nil
}

//
// do all of the supplied keys exist in the cache
//
func (ci *cacheProxyImpl) Exists(keys []string) (bool, error) {

	var ids []struct {
		ID string `db:"id"`
	}

	q := ci.db.Select("id").
		From(ci.tableName).
		Where(dbx.In("id", toInterfaceArray(keys)...))

	start := time.Now()
	err := q.All(&ids)
	elapsed := int64(time.Since(start) / time.Millisecond)
	ci.warnIfSlow(elapsed, lookupRequestTimeLimit, fmt.Sprintf("CacheExists (%d items)", len(keys)))

	if err != nil {
		return false, err
	}

	//log.Printf("INFO: lookup %d keys in %d milliseconds (%d results)", len( keys ), elapsed, len(ids))

	// verify that we received all ids
	if len(ids) != len(keys) {

		// locate which key(s) were not located and log as necessary
		founds := make([]bool, len(keys))
		for _, id := range ids {
			ix, found := find(keys, id.ID)
			if found {
				founds[ix] = true
			}
		}

		for ix, f := range founds {
			if f == false {
				log.Printf("ERROR: id %s does not exist in the cache", keys[ix])
			}
		}

		return false, ErrNotInCache
	}

	// everything OK
	return true, nil
}

//
// get the specified items from the cache
//
func (ci *cacheProxyImpl) Get(keys []string) ([]awssqs.Message, error) {

	var cacheRecords []struct {
		ID      string `db:"id"`
		Type    string `db:"type"`
		Source  string `db:"source"`
		Payload string `db:"payload"`
	}

	q := ci.db.Select("id", "type", "source", "payload").
		From(ci.tableName).
		Where(dbx.In("id", toInterfaceArray(keys)...))

	start := time.Now()
	err := q.All(&cacheRecords)
	elapsed := int64(time.Since(start) / time.Millisecond)
	ci.warnIfSlow(elapsed, getRequestTimeLimit, fmt.Sprintf("CacheGet (%d items)", len(keys)))

	//log.Printf("INFO: get %d keys in %d milliseconds (%d results)", len( keys ), elapsed, len(cacheRecords))

	if err != nil {
		return nil, err
	}

	// verify that we received all ids
	if len(cacheRecords) != len(keys) {
		log.Printf("ERROR: item not found during cache lookup, this is unexpected")
		return nil, ErrNotInCache
	}

	// the response
	messages := make([]awssqs.Message, 0, len(keys))

	for _, r := range cacheRecords {

		if len(r.ID) == 0 {
			log.Printf("WARNING: id is empty")
		}

		if len(r.Type) == 0 {
			log.Printf("WARNING: type is empty")
		}

		if len(r.Source) == 0 {
			log.Printf("WARNING: source is empty")
		}

		if len(r.Payload) == 0 {
			log.Printf("WARNING: payload is empty")
		}

		//log.Printf( "Record %d: ID:         %s", ix, r.ID )
		//log.Printf( "Record %d: datatype:   %s", ix, r.Type )
		//log.Printf( "Record %d: datasource: %s", ix, r.Source )
		//log.Printf( "Record %d: payload:    %s", ix, r.Payload )

		messages = append(messages, *ci.constructMessage(r.ID, r.Type, r.Source, r.Payload))
	}

	return messages, nil
}

// construct the outbound SQS message
func (ci *cacheProxyImpl) constructMessage(id string, theType string, source string, payload string) *awssqs.Message {

	attributes := make([]awssqs.Attribute, 0, 4)
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordId, Value: id})
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordType, Value: theType})
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordSource, Value: source})
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordOperation, Value: awssqs.AttributeValueRecordOperationUpdate})
	return &awssqs.Message{Attribs: attributes, Payload: []byte(payload)}
}

// sometimes it is interesting to know if our SQS queries are slow
func (ci *cacheProxyImpl) warnIfSlow(elapsed int64, limit int64, prefix string) {

	if elapsed > limit {
		log.Printf("INFO: %s elapsed %d ms", prefix, elapsed)
	}
}

// simplified hack
func find(slice []string, value string) (int, bool) {
	for ix, item := range slice {
		if item == value {
			return ix, true
		}
	}
	return -1, false
}

func toInterfaceArray(strings []string) []interface{} {
	ia := make([]interface{}, len(strings))
	for ix, v := range strings {
		ia[ix] = v
	}
	return ia
}

//
// end of file
//
