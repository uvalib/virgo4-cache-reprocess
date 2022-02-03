package main

import (
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
	"log"
	"time"
)

func cache_worker(id int, cache CacheProxy, inbound <-chan Record, outbound chan<- awssqs.Message) {

	// we get from the cache in blocks
	bsize := uint(getCacheMaxKeyCount)
	count := uint(0)
	block := make([]Record, 0, bsize)
	var record Record
	for {

		timeout := false

		// process a message or wait...
		select {
		case record = <-inbound:

		case <-time.After(flushTimeout):
			timeout = true
		}

		// did we timeout, if not we have a message to process
		if timeout == false {

			block = append(block, record)

			// have we reached a block size limit
			if count != 0 && count%bsize == bsize-1 {

				// get a batch of records from the cache
				messages, err := batchCacheGet(cache, block)
				fatalIfError(err)

				// and send them to the outbound queue
				for _, m := range messages {
					outbound <- m
				}

				// reset the block
				block = block[:0]
			}
			count++

			if count%1000 == 0 {
				log.Printf("INFO: cache worker %d processed %d records", id, count)
			}
		} else {

			// we timed out waiting for new messages, let's flush what we have (if anything)
			if len(block) != 0 {

				messages, err := batchCacheGet(cache, block)
				fatalIfError(err)

				// and send them to the outbound queue
				for _, m := range messages {
					outbound <- m
				}

				// reset the block
				block = block[:0]

				log.Printf("INFO: cache worker %d processed %d records (flushing)", id, count)
			}

			// reset the count
			count = 0
		}
	}

	// should never get here
}

// look up a set of keys in the cache. We have already verified that the keys all exist so we expect failures to
// be fatal
func batchCacheGet(cache CacheProxy, records []Record) ([]awssqs.Message, error) {

	keys := make([]string, 0, len(records))
	for _, m := range records {
		keys = append(keys, m.Id())
	}

	messages, err := cache.Get(keys)
	if err != nil {
		return nil, err
	}

	return messages, nil
}

//
// end of file
//
