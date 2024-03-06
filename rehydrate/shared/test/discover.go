package test

import (
	"github.com/pennsieve/pennsieve-go/pkg/pennsieve/models/authentication"
	"github.com/stretchr/testify/require"
	"net/http"
)

var defaultCognitoConfig = authentication.CognitoConfig{
	Region: "us-east-1",
	UserPool: authentication.UserPool{
		Region:      "us-east-1",
		ID:          "mock-user-pool-id",
		AppClientID: "mock-user-pool-app-client-id",
	},
	TokenPool: authentication.TokenPool{
		Region:      "us-east-1",
		AppClientID: "mockTokenPoolAppClientId",
	},
	IdentityPool: authentication.IdentityPool{
		Region: "us-east-1",
		ID:     "mock-identity-pool-id",
	}}

type DiscoverServerFixture struct {
	*HTTPMuxTestFixture
	T require.TestingT
}

// NewDiscoverServerFixture A wrapper around httptest.Server for mocking Discover service responses.
// Written for Go 1.21, so before methods and wildcards were part of http.ServeMux patterns.
func NewDiscoverServerFixture(t require.TestingT, cognitoConfig *authentication.CognitoConfig) *DiscoverServerFixture {
	fixture := NewHTTPMuxTestFixture(t)
	fixture.Mux.HandleFunc("/authentication/cognito-config", func(writer http.ResponseWriter, request *http.Request) {
		require.Equal(t, http.MethodGet, request.Method)
		model := cognitoConfig
		if model == nil {
			model = &defaultCognitoConfig
		}
		fixture.WriteResponseModel(writer, model)
	})
	return &DiscoverServerFixture{T: t, HTTPMuxTestFixture: fixture}
}

func (d *DiscoverServerFixture) HandlerFunc(pattern string, handlerFunc http.HandlerFunc) {
	d.Mux.HandleFunc(pattern, handlerFunc)
}
