package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
)

var BadRecordError = fmt.Errorf("Bad record encountered")
var BadRecordIdError = fmt.Errorf("Bad record identifier")
var FileNotOpenError = fmt.Errorf("File is not open")

// the RecordLoader interface
type RecordLoader interface {
	Validate() error
	First( ) (Record, error)
	Next( ) (Record, error)
	Done()
}

// the record interface
type Record interface {
	Id() (string, error)
	Raw() []byte
}

// this is our loader implementation
type recordLoaderImpl struct {
	File       *os.File
	Reader     *bufio.Reader
}

// this is our record implementation
type recordImpl struct {
	RawBytes []byte
	recordId string
}

// and the factory
func NewRecordLoader(filename string) (RecordLoader, error) {

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader( file )

	return &recordLoaderImpl{File: file, Reader: reader}, nil
}

// read all the records to ensure the file is valid
func (l *recordLoaderImpl) Validate() error {

	if l.File == nil {
		return FileNotOpenError
	}

	// get the first record and error out if bad. An EOF is OK, just means the file is empty
	_, err := l.First( )
	if err != nil {
		// are we done
		if err == io.EOF {
			log.Printf("WARNING: EOF on first read, looks like an empty file")
			return nil
		} else {
			return err
		}
	}

	// read all the records and bail on the first failure except EOF
	for {
		_, err = l.Next( )

		if err != nil {
			// are we done
			if err == io.EOF {
				break
			} else {
				return err
			}
		}
	}

	// everything is OK
	return nil
}

func (l *recordLoaderImpl) First( ) (Record, error) {

	if l.File == nil {
		return nil, FileNotOpenError
	}

	// go to the start of the file and then get the next record
	_, err := l.File.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	return l.Next( )
}

func (l *recordLoaderImpl) Next( ) (Record, error) {

	if l.File == nil {
		return nil, FileNotOpenError
	}

	rec, err := l.recordRead()
	if err != nil {
		return nil, err
	}

	_, err = rec.Id()
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


	line, err := l.Reader.ReadString( '\n' )
	if err != nil {
		return nil, err
	}

	if len( line ) == 0 {
		return nil, BadRecordError
	}

	return &recordImpl{RawBytes: []byte( line )}, nil
}

func (r *recordImpl) Id() (string, error) {

	if len( r.recordId ) != 0 {
		return r.recordId, nil
	}

	return r.extractId()
}

func (r *recordImpl) Raw() []byte {
	return r.RawBytes
}

func (r *recordImpl) extractId() (string, error) {

	id := string( r.RawBytes )

	// ensure the first character of the Id us a 'u' character
	if id[0] != 'u' {
		log.Printf("ERROR: record id is suspect (%s)", id)
		return "", BadRecordIdError
	}

	r.recordId = id

	return r.recordId, nil
}

//
// end of file
//
