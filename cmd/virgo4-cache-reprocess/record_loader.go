package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

var ErrBadRecord = fmt.Errorf("bad record encountered")
var ErrBadRecordId = fmt.Errorf("bad record identifier")
var ErrRecordNotInCache = fmt.Errorf("record is not available in the cache")
var ErrFileNotOpen = fmt.Errorf("file is not open")

// the RecordLoader interface
type RecordLoader interface {
	Validate(CacheProxy) error
	First() (Record, error)
	Next() (Record, error)
	Done()
}

// the record interface
type Record interface {
	Id() string
	//Raw() []byte
}

// this is our loader implementation
type recordLoaderImpl struct {
	File   *os.File
	Reader *bufio.Reader
}

// this is our record implementation
type recordImpl struct {
	//RawBytes []byte
	RecordId string
}

// and the factory
func NewRecordLoader(filename string) (RecordLoader, error) {

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(file)

	return &recordLoaderImpl{File: file, Reader: reader}, nil
}

// read all the records to ensure the file is valid
func (l *recordLoaderImpl) Validate(cache CacheProxy) error {

	if l.File == nil {
		return ErrFileNotOpen
	}

	// get the first record and error out if bad. An EOF is OK, just means the file is empty
	rec, err := l.First()
	if err != nil {
		// are we done
		if err == io.EOF {
			log.Printf("WARNING: EOF on first read, looks like an empty file")
			return nil
		} else {
			log.Printf("ERROR: validation failure on record index 0")
			return err
		}
	}

	// batch up our cache lookups for performance reasons
	lookupIds := make([]string, 0, lookupCacheMaxKeyCount)
	lookupIds = append(lookupIds, rec.Id())

	// used for reporting
	recordIndex := 1

	// read all the records and bail on the first failure except EOF
	for {
		rec, err = l.Next()

		if err != nil {
			// are we done
			if err == io.EOF {
				break
			} else {
				log.Printf("ERROR: validation failure on record index %d", recordIndex)
				return err
			}
		}

		lookupIds = append(lookupIds, rec.Id())
		if len(lookupIds) == lookupCacheMaxKeyCount {

			// lookup in the cache
			found, err := cache.Exists(lookupIds)
			if err != nil {
				return err
			}

			// if we cannot find it in the cache, its an error
			if found == false {
				startIx := 0
				if recordIndex > lookupCacheMaxKeyCount {
					startIx = recordIndex - lookupCacheMaxKeyCount
				}
				log.Printf("ERROR: one or more records not in cache between index %d and %d", startIx, recordIndex)
				return ErrRecordNotInCache
			}

			lookupIds = lookupIds[:0]
		}
		recordIndex++
	}

	sz := len(lookupIds)
	if sz != 0 {

		// lookup in the cache
		found, err := cache.Exists(lookupIds)
		if err != nil {
			return err
		}

		// if we cannot find it in the cache, its an error
		if found == false {
			startIx := 0
			if recordIndex > sz {
				startIx = recordIndex - sz
			}
			log.Printf("ERROR: one or more records not in cache between index %d and %d", startIx, recordIndex)
			return ErrRecordNotInCache
		}
	}

	// everything is OK
	return nil
}

func (l *recordLoaderImpl) First() (Record, error) {

	if l.File == nil {
		return nil, ErrFileNotOpen
	}

	// go to the start of the file and then get the next record
	_, err := l.File.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	return l.Next()
}

func (l *recordLoaderImpl) Next() (Record, error) {

	if l.File == nil {
		return nil, ErrFileNotOpen
	}

	rec, err := l.recordRead()
	if err != nil {
		return nil, err
	}

	return rec, nil
}

func (l *recordLoaderImpl) Done() {

	if l.File != nil {
		l.File.Close()
		l.File = nil
	}
}

func (l *recordLoaderImpl) recordRead() (Record, error) {

	id, err := l.Reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	// remove the newline
	id = strings.TrimSuffix(id, "\n")

	if len(id) == 0 {
		return nil, ErrBadRecord
	}

	if id[0] != 'u' {
		log.Printf("ERROR: record id is suspect (%s)", id)
		return nil, ErrBadRecordId
	}

	return &recordImpl{RecordId: id}, nil
}

func (r *recordImpl) Id() string {
	return r.RecordId
}

//func (r *recordImpl) Raw() []byte {
//	return r.RawBytes
//}

//
// end of file
//
