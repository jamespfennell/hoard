package util

import "strings"

type multipleError struct {
	errs []error
}

func NewMultipleError(errs ...error) error {
	var cleanedErrs []error
	for _, err := range errs {
		if err == nil {
			continue
		}
		cleanedErrs = append(cleanedErrs, err)
	}
	if len(cleanedErrs) == 0 {
		return nil
	}
	return multipleError{errs: cleanedErrs}
}

func (err multipleError) Error() string {
	var b strings.Builder
	b.WriteString("multiple errors encountered:")
	for _, e := range err.errs {
		b.WriteString("\n - ")
		b.WriteString(e.Error())
	}
	return b.String()
}
