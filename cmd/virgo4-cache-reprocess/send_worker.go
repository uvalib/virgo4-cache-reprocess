package main

import (
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
	"log"
	"time"
)

func send_worker(id int, config ServiceConfig, aws awssqs.AWS_SQS, queue awssqs.QueueHandle, tosend <-chan awssqs.Message) {

	// we send to the outbound queue in blocks
	bsize := awssqs.MAX_SQS_BLOCK_COUNT
	count := uint(0)
	messages := make([]awssqs.Message, 0, bsize)
	var record awssqs.Message
	for {

		timeout := false

		// process a message or wait...
		select {
		case record = <-tosend:

		case <-time.After(flushTimeout):
			timeout = true
		}

		// did we timeout, if not we have a message to process
		if timeout == false {

			messages = append(messages, record)

			// have we reached a block size limit
			if count != 0 && count%bsize == bsize-1 {

				// send the block
				err := sendOutboundMessages(config, aws, queue, messages)
				if err != nil {
					if err != awssqs.ErrOneOrMoreOperationsUnsuccessful {
						fatalIfError(err)
					}
				}

				// reset the block
				messages = messages[:0]
			}
			count++

			if count%1000 == 0 {
				log.Printf("send worker %d processed %d records", id, count)
			}
		} else {

			// we timed out waiting for new messages, let's flush what we have (if anything)
			if len(messages) != 0 {

				// send the block
				err := sendOutboundMessages(config, aws, queue, messages)
				if err != nil {
					if err != awssqs.ErrOneOrMoreOperationsUnsuccessful {
						fatalIfError(err)
					}
				}

				// reset the block
				messages = messages[:0]

				log.Printf("send worker %d processed %d records (flushing)", id, count)
			}

			// reset the count
			count = 0
		}
	}

	// should never get here
}

func sendOutboundMessages(config ServiceConfig, aws awssqs.AWS_SQS, queue awssqs.QueueHandle, batch []awssqs.Message) error {

	opStatus, err := aws.BatchMessagePut(queue, batch)
	if err != nil {
		if err != awssqs.ErrOneOrMoreOperationsUnsuccessful {
			return err
		}
	}

	// if one or more message failed to send, retry...
	if err == awssqs.ErrOneOrMoreOperationsUnsuccessful {

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
