package test

import "testing"

type EnvironmentVariables struct {
	env map[string]string
}

func NewEnvironmentVariables() *EnvironmentVariables {
	return &EnvironmentVariables{map[string]string{}}
}

func (e *EnvironmentVariables) With(key, value string) *EnvironmentVariables {
	e.env[key] = value
	return e
}

func (e *EnvironmentVariables) Setenv(t *testing.T) {
	for k, v := range e.env {
		t.Setenv(k, v)
	}
}
