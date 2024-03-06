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
	m.Mux.HandleFunc(pattern, func(writer http.ResponseWriter, request *http.Request) {
		require.Equal(m.TestingT, method, request.Method)
		m.WriteResponseModel(writer, model)
	})
}

func (m *HTTPMuxTestFixture) MultiModelHandlerFunction(method, pattern string, modelSelectorFunc func(r *http.Request) any) {
	m.Mux.HandleFunc(pattern, func(writer http.ResponseWriter, request *http.Request) {
		require.Equal(m.TestingT, method, request.Method)
		model := modelSelectorFunc(request)
		require.NotNil(m.TestingT, model, "no model selected for request: method: %s, path: %s, query params: %s", request.Method, request.URL.Path, request.URL.Query())
		m.WriteResponseModel(writer, model)
	})
}

func (m *HTTPMuxTestFixture) WriteResponseModel(writer http.ResponseWriter, responseModel any) {
	respBody, err := json.Marshal(responseModel)
	require.NoError(m.TestingT, err)
	written, err := writer.Write(respBody)
	require.NoError(m.TestingT, err)
	require.Equal(m.TestingT, len(respBody), written)
}
