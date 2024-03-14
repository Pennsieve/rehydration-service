package test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
)

type RequestAssertionFunc func(t require.TestingT, request *http.Request) bool

type HTTPTestResponse struct {
	Status int
	Body   string
}

type HTTPTestFixture struct {
	Server *httptest.Server
}

// NewHTTPTestFixture returns a pointer to a new HTTPTestFixture
// If response is nil, any requests made to the fixture's server will fail the test.
// If reqAssertionFunc is non-nil and returns false for a given http.Request, the fixture will fail the test.
// If reqAssertionFunc is nil or returns true, the fixture's server will return the status and body contained in response
// Passing a nil response and non-nil reqAssertionFunc to this function is an error and will cause the test to fail.
func NewHTTPTestFixture(t require.TestingT, reqAssertionFunc RequestAssertionFunc, response *HTTPTestResponse) HTTPTestFixture {
	require.False(t, reqAssertionFunc != nil && response == nil,
		"cannot have nil response with non-nil reqAssertionFunc; nil response implies that reqAssertionFunc will never be called")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {

		if response == nil {
			requestBody := requestToString(t, request.Body)
			assert.FailNow(t, "unexpected call to HTTPTestFixture", "request body: %s", requestBody)
		}

		if reqAssertionFunc != nil && !reqAssertionFunc(t, request) {
			assert.FailNow(t, "RequestAssertionFunc failed")
		}
		if response.Status != 0 {
			writer.WriteHeader(response.Status)
		}
		written, err := fmt.Fprintln(writer, response.Body)
		require.NoError(t, err)
		// +1 for the newline
		require.Equal(t, len(response.Body)+1, written)

	}))
	return HTTPTestFixture{
		Server: server,
	}
}

func requestToString(t require.TestingT, body io.ReadCloser) string {
	requestBody, err := io.ReadAll(body)
	require.NoError(t, err)
	return string(requestBody)
}

func (h HTTPTestFixture) Teardown() {
	if h.Server != nil {
		h.Server.Close()
	}
}
