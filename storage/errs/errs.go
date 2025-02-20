package errs

import (
	"errors"
	"fmt"
	"log"
)

var (
	ErrFieldTypeConflict = errors.New("field types conflict")
)

func Check(err error) {
	if err != nil {
		log.Fatalf("%+v", err)
	}
}

func Check2(_ interface{}, err error) {
	Check(err)
}

func CheckArgument(b bool) {
	if !b {
		log.Fatalf("%+v", fmt.Errorf("assert failed"))
	}
}

func CheckArgumentf(b bool, format string, args ...interface{}) {
	if !b {
		log.Fatalf("%+v", fmt.Errorf(format, args...))
	}
}

func Error(err error, msg string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s errs: %+v", msg, err)
}

func Errorf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf(format+" errs: %+v", append(args, err)...)
}
