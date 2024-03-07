package test

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
)

type HTTPMuxTestFixture struct {
	Server   *httptest.Server
	Mux      *http.ServeMux
	TestingT require.TestingT
}

func NewHTTPMuxTestFixture(t require.TestingT) *HTTPMuxTestFixture {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
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

func (m *HTTPMuxTestFixture) ModelHandlerFunc(method, pattern string, model any) {
	m.Mux.HandleFunc(NewHandlerFuncBuilder(pattern).WithMethod(method).WithModel(model).Build(m.TestingT))
}

func (m *HTTPMuxTestFixture) MultiModelHandlerFunction(method, pattern string, modelSelectorFunc func(r *http.Request) any) {
	m.Mux.HandleFunc(NewHandlerFuncBuilder(pattern).WithMethod(method).WithSelectorFunc(func(r *http.Request) (int, any) {
		return http.StatusOK, modelSelectorFunc(r)
	}).Build(m.TestingT))
}

// WriteResponseModel uses writer to write responseModel. If responseModel is not a string or []byte, it is passed to
// json.Marshall to convert it to []byte
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

type HandlerFuncBuilder struct {
	method       string
	pattern      string
	statusCode   int
	model        any
	selectorFunc func(r *http.Request) (int, any)
}

func NewHandlerFuncBuilder(pattern string) *HandlerFuncBuilder {
	return &HandlerFuncBuilder{
		method:     http.MethodGet,
		pattern:    pattern,
		statusCode: http.StatusOK,
	}
}

func (b *HandlerFuncBuilder) WithMethod(method string) *HandlerFuncBuilder {
	b.method = method
	return b
}

func (b *HandlerFuncBuilder) WithStatusCode(statusCode int) *HandlerFuncBuilder {
	b.statusCode = statusCode
	return b
}

func (b *HandlerFuncBuilder) WithModel(model any) *HandlerFuncBuilder {
	b.model = model
	return b
}

func (b *HandlerFuncBuilder) WithSelectorFunc(selectorFunc func(r *http.Request) (int, any)) *HandlerFuncBuilder {
	b.selectorFunc = selectorFunc
	// b.statusCode will be ignored if a selector function is set
	b.statusCode = 0
	return b
}

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
