package main

import (
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
	"log"
	"time"
)

// time to wait before flushing pending records
var flushTimeout = 5 * time.Second

func worker(id int, config ServiceConfig, aws awssqs.AWS_SQS, cache CacheProxy, queue awssqs.QueueHandle, records <-chan Record) {

	count := uint(1)
	block := make([]Record, 0, awssqs.MAX_SQS_BLOCK_COUNT)
	var record Record
	for {

		timeout := false

		// process a message or wait...
		select {
		case record = <-records:
			break
		case <-time.After(flushTimeout):
			timeout = true
			break
		}

		// did we timeout, if not we have a message to process
		if timeout == false {

			block = append(block, record)

			// have we reached a block size limit
			if count%awssqs.MAX_SQS_BLOCK_COUNT == 0 {

				// get a batch of records from the cache
				messages, err := batchCacheGet(cache, block)
				fatalIfError(err)

				// send the block
				err = sendOutboundMessages(config, aws, queue, messages)
				if err != nil {
					if err != awssqs.OneOrMoreOperationsUnsuccessfulError {
						fatalIfError(err)
					}
				}

				// reset the block
				block = block[:0]
			}
			count++

			if count%1000 == 0 {
				log.Printf("Worker %d processed %d records", id, count)
			}
		} else {

			// we timed out waiting for new messages, let's flush what we have (if anything)
			if len(block) != 0 {

				messages, err := batchCacheGet(cache, block)
				fatalIfError(err)

				// send the block
				err = sendOutboundMessages(config, aws, queue, messages)
				if err != nil {
					if err != awssqs.OneOrMoreOperationsUnsuccessfulError {
						fatalIfError(err)
					}
				}

				// reset the block
				block = block[:0]

				log.Printf("Worker %d processed %d records (flushing)", id, count)
			}

			// reset the count
			count = 1
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

	// special case
	if len(messages) != len(records) {
		log.Printf("WARNING: not all cache gets were successful, this is unexpected")
	}

	return messages, nil
}

func sendOutboundMessages(config ServiceConfig, aws awssqs.AWS_SQS, queue awssqs.QueueHandle, batch []awssqs.Message) error {

	opStatus, err := aws.BatchMessagePut(queue, batch)
	if err != nil {
		if err != awssqs.OneOrMoreOperationsUnsuccessfulError {
			return err
		}
	}

	// if one or more message failed to send, retry...
	if err == awssqs.OneOrMoreOperationsUnsuccessfulError {

		// check the operation results
		for ix, op := range opStatus {
			if op == false {
				log.Printf("WARNING: message %d failed to send to queue", ix)
			}
		}
	}

	return err
}

//
// end of file
//
