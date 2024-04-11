package lambdautils

import (
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambdacontext"
)

func ErrorBody(err error, lambdaRequest events.APIGatewayV2HTTPRequest) string {
	return fmt.Sprintf(`{"requestID": %q, "logStream": %q, "message": %q}`, lambdaRequest.RequestContext.RequestID, lambdacontext.LogStreamName, err)
}

func ErrorResponse(statusCode int, err error, lambdaRequest events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	return events.APIGatewayV2HTTPResponse{
		StatusCode: statusCode,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       ErrorBody(err, lambdaRequest),
	}, nil
}
