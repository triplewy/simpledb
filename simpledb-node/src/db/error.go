package db

type errWriteUnexpectedBytes struct {
	file string
}

func newErrWriteUnexpectedBytes(file string) *errWriteUnexpectedBytes {
	return &errWriteUnexpectedBytes{file: file}
}
func (e *errWriteUnexpectedBytes) Error() string {
	return "Wrote unexpected amount of bytes to " + e.file
}

type errReadUnexpectedBytes struct {
	file string
}

func newErrReadUnexpectedBytes(file string) *errReadUnexpectedBytes {
	return &errReadUnexpectedBytes{file: file}
}
func (e *errReadUnexpectedBytes) Error() string {
	return "Read unexpected amount of bytes from " + e.file
}

type errKeyNotFound struct{}

func newErrKeyNotFound() *errKeyNotFound {
	return &errKeyNotFound{}
}
func (e *errKeyNotFound) Error() string {
	return "Key not found"
}

type errIncorrectBlockSize struct{}

func newErrIncorrectBlockSize() *errIncorrectBlockSize {
	return &errIncorrectBlockSize{}
}

func (e *errIncorrectBlockSize) Error() string {
	return "Data block does not match block size"
}
