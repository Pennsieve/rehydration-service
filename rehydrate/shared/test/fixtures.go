package test

import (
	"sync"
	"testing"
)

type Fixture struct {
	T *testing.T // this should really be a require.TestingT from testify to make this and the types that include it themselves testable
}

func waitForEverything[T any](inputs []T, waitFn func(T) error) error {
	var wg sync.WaitGroup
	waitErrors := make([]error, len(inputs))
	for index, input := range inputs {
		wg.Add(1)
		go func(i int, in T) {
			defer wg.Done()
			waitErrors[i] = waitFn(in)
		}(index, input)
	}
	wg.Wait()
	for _, we := range waitErrors {
		if we != nil {
			return we
		}
	}
	return nil
}
