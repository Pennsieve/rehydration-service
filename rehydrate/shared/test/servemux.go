package test

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
)

// HTTPMuxTestFixture is a wrapper around a httptest.Server with a http.ServeMux
// It is used to create a test server that can take multiple independent http.HandlerFunc, each handling a different path.
type HTTPMuxTestFixture struct {
	Server   *httptest.Server
	Mux      *http.ServeMux
	TestingT require.TestingT
}

// NewHTTPMuxTestFixture returns a *HTTPMuxTestFixture configured to fail if it receives any request it does not
// have a handler for.
// Add handlers by calling HTTPMuxTestFixture.ModelHandleFunc or HTTPMuxTestFixture.MultiModelHandleFunc. Or by
// adding to the wrapped http.ServeMux directly
func NewHTTPMuxTestFixture(t require.TestingT, handlerBuilders ...*HandlerFuncBuilder) *HTTPMuxTestFixture {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	for _, b := range handlerBuilders {
		mux.HandleFunc(b.Build(t))
	}
	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		request.URL.Query()
		require.FailNowf(t, "unhandled request; if this is an expected request, add a handler to this fixture",
			"method: %s, path: %s, query params: %s", request.Method, request.URL.Path, request.URL.Query())
	})
	return &HTTPMuxTestFixture{
		Server:   server,
		Mux:      mux,
		TestingT: t,
	}
}

func (m *HTTPMuxTestFixture) Teardown() {
	m.Server.Close()
}

// ModelHandleFunc registers a http.HandlerFunc for the given pattern and method that always returns the given model with a HTTP
// status of http.StatusOK.
//
// It will fail the test if a request is received for the given pattern that does not have the given method.
// This was written for Go < 1.22 before methods became part of patterns
// If model is not a string or []byte, json.Unmarshal is used to create a []byte the response.
func (m *HTTPMuxTestFixture) ModelHandleFunc(method, pattern string, model any) {
	m.Mux.HandleFunc(NewHandlerFuncBuilder(pattern).WithMethod(method).WithModel(model).Build(m.TestingT))
}

// MultiModelHandleFunc registers a http.HandlerFunc for the given pattern and method that calls the given selector
// function to determine the model that should be returned. The response's HTTP status code will be http.StatusOK.
//
// It will fail the test if the selector function returns nil.
//
// It will fail the test if a request is received for the given pattern that does not have the given method.
//
// This was written for Go < 1.22 before methods became part of patterns
//
// If the model returned by the selector function is not a string or []byte, json.Unmarshal is used to create a []byte the response.
func (m *HTTPMuxTestFixture) MultiModelHandleFunc(method, pattern string, modelSelectorFunc func(r *http.Request) any) {
	m.Mux.HandleFunc(NewHandlerFuncBuilder(pattern).WithMethod(method).WithSelectorFunc(func(r *http.Request) (int, any) {
		return http.StatusOK, modelSelectorFunc(r)
	}).Build(m.TestingT))
}

// WriteResponseModel uses writer to write responseModel. If responseModel is not a string or []byte, it is passed to
// json.Marshall to convert it to []byte for writing
func (m *HTTPMuxTestFixture) WriteResponseModel(writer http.ResponseWriter, responseModel any) {
	writeResponseModel(m.TestingT, writer, http.StatusOK, responseModel)
}

func writeResponseModel(t require.TestingT, writer http.ResponseWriter, statusCode int, responseModel any) {
	var respBytes []byte
	switch r := responseModel.(type) {
	case []byte:
		respBytes = r
	case string:
		respBytes = []byte(r)
	default:
		var err error
		respBytes, err = json.Marshal(r)
		require.NoError(t, err)
	}
	writer.WriteHeader(statusCode)
	written, err := writer.Write(respBytes)
	require.NoError(t, err)
	require.Equal(t, len(respBytes), written)
}

// HandlerFuncBuilder is used to build (pattern string, func http.HandlerFunc) pairs that can be passed to http.ServeMux.HandleFunc()
type HandlerFuncBuilder struct {
	method       string
	pattern      string
	statusCode   int
	model        any
	selectorFunc func(r *http.Request) (int, any)
}

// NewHandlerFuncBuilder returns a *HandlerFuncBuilder for the given pattern with a default method of http.MethodGet
// satusCode of http.StatusOK
func NewHandlerFuncBuilder(pattern string) *HandlerFuncBuilder {
	return &HandlerFuncBuilder{
		method:     http.MethodGet,
		pattern:    pattern,
		statusCode: http.StatusOK,
	}
}

// WithMethod sets this builder's method. The built http.HandlerFunc will fail the test if a
// request is received matching this builder's pattern, but with a different method.
func (b *HandlerFuncBuilder) WithMethod(method string) *HandlerFuncBuilder {
	b.method = method
	return b
}

// WithStatusCode sets this builder's statusCode.
//
// If WithModel is called, the built http.HandlerFunc
// will respond with the given statusCode and the given model for any request matching this builder's
// pattern and method.
//
// If WithSelectorFunc is called this statusCode will be ignored in favor of the status code returned by
// the given selector function
func (b *HandlerFuncBuilder) WithStatusCode(statusCode int) *HandlerFuncBuilder {
	b.statusCode = statusCode
	return b
}

// WithModel sets this builder's model.
//
// The built http.HandlerFunc will return this model for any request matching this builder's pattern and method.
// The model can be string, []byte, or an object that json.Marshal can turn into a []byte
//
// It is an error to call both WithModel and WithSelectorFunc.
func (b *HandlerFuncBuilder) WithModel(model any) *HandlerFuncBuilder {
	b.model = model
	return b
}

// WithSelectorFunc sets this builder's selector function.
//
// The built http.HandlerFunc will use this function to map the http.Request object to a (statusCode, model) pair that
// will be used to create the response to a request matching this builder's pattern and method.
//
// The model returned by selectorFunc can be string, []byte, or an object that json.Marshal can turn into a []byte
//
// It is an error to call both WithModel and WithSelectorFunc.
func (b *HandlerFuncBuilder) WithSelectorFunc(selectorFunc func(r *http.Request) (int, any)) *HandlerFuncBuilder {
	b.selectorFunc = selectorFunc
	// b.statusCode will be ignored if a selector function is set
	b.statusCode = 0
	return b
}

// Build returns a (string, http.HandlerFunc) pair. The string is this builder's pattern.
// The http.Handler func will check that the http.Request's method matches this builder's method.
// It will either respond with this builder's statusCode and model or use this builder's selector function
// to map the http.Request to a statusCode and model.
func (b *HandlerFuncBuilder) Build(t require.TestingT) (string, http.HandlerFunc) {
	require.NotEmpty(t, b.method, "method may not be empty")
	require.NotEmpty(t, b.pattern, "pattern may not be empty")
	var selectorFunction func(r *http.Request) (int, any)
	if b.selectorFunc != nil {
		require.True(t, b.model == nil && b.statusCode == 0, "if selectorFunc is not nil you cannot set model or statusCode")
		selectorFunction = b.selectorFunc
	} else {
		require.NotNil(t, b.model, "either set a selectorFunc or a model")
		selectorFunction = func(r *http.Request) (int, any) {
			return b.statusCode, b.model
		}
	}
	return b.pattern, func(writer http.ResponseWriter, request *http.Request) {
		require.Equal(t, b.method, request.Method)
		statusCode, responseModel := selectorFunction(request)
		require.NotNil(t, responseModel, "no responseModel selected for request: method: %s, path: %s, query params: %s", request.Method, request.URL.Path, request.URL.Query())
		writeResponseModel(t, writer, statusCode, responseModel)
	}
}
