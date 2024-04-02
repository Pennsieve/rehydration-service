package dydbutils

import "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

func StringAttributeValue(attributeValue string) types.AttributeValue {
	return &types.AttributeValueMemberS{Value: attributeValue}
}
