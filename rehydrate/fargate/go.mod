module github.com/pennsieve/rehydration-service/fargate

go 1.21

replace github.com/pennsieve/rehydration-service/shared => ../../shared

require (
	github.com/aws/aws-sdk-go v1.45.23
	github.com/aws/aws-sdk-go-v2 v1.21.0
	github.com/aws/aws-sdk-go-v2/config v1.15.7
	github.com/aws/aws-sdk-go-v2/service/s3 v1.40.0
	github.com/pennsieve/pennsieve-go v1.3.1
	github.com/pennsieve/rehydration-service/shared v0.0.0-00010101000000-000000000000
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.4.13 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.12.2 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.12.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.41 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.35 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.12 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.1.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/cognitoidentity v1.14.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider v1.20.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.14 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.1.36 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.35 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.15.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.11.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.16.6 // indirect
	github.com/aws/smithy-go v1.14.2 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/pennsieve/pennsieve-go-api v1.1.0 // indirect
)
