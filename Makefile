.PHONY: help clean test package publish test-ci

LAMBDA_BUCKET ?= "pennsieve-cc-lambda-functions-use1"
WORKING_DIR   ?= "$(shell pwd)"
SERVICE_NAME  ?= "rehydration-service"
LAMBDA_BIN ?= $(WORKING_DIR)/lambda/bin
SERVICE_PACKAGE_NAME ?= "rehydration-service-${IMAGE_TAG}.zip"

.DEFAULT: help

help:
	@echo "Make Help for $(SERVICE_NAME)"
	@echo ""
	@echo "make package - create venv and package lambdas and fargate functions"
	@echo "make publish - package and publish lambda function"

go-get:
	cd $(WORKING_DIR)/lambda/service; \
		go get github.com/pennsieve/rehydration-service/service
	cd $(WORKING_DIR)/rehydrate/fargate; \
		go get github.com/pennsieve/rehydration-service/fargate

test-ci:
	@echo ""

# Spin down active docker containers.
docker-clean:		
	@echo ""

clean:
	rm -fr $(LAMBDA_BIN)

package:
	@echo ""
	@echo "***********************"
	@echo "*   Building Rehydrate Trigger lambda   *"
	@echo "***********************"
	@echo ""
	cd $(WORKING_DIR)/lambda/service; \
  		env GOOS=linux GOARCH=arm64 go build -tags lambda.norpc -o $(LAMBDA_BIN)/service/bootstrap; \
		cd $(LAMBDA_BIN)/service/ ; \
			zip -r $(LAMBDA_BIN)/service/$(SERVICE_PACKAGE_NAME) .
	@echo ""
	@echo "***********************"
	@echo "*   Building Fargate   *"
	@echo "***********************"
	@echo ""
	cd $(WORKING_DIR)/rehydrate; \
		docker build -t pennsieve/rehydrate:${IMAGE_TAG} . ;\

publish:
	@make package
	@echo ""
	@echo "*************************"
	@echo "*   Publishing Trigger lambda   *"
	@echo "*************************"
	@echo ""
	aws s3 cp $(LAMBDA_BIN)/service/$(SERVICE_PACKAGE_NAME) s3://$(LAMBDA_BUCKET)/$(SERVICE_NAME)/service/
	rm -rf $(LAMBDA_BIN)/service/$(SERVICE_PACKAGE_NAME)
	@echo ""
	@echo "***********************"
	@echo "*   Publishing Fargate   *"
	@echo "***********************"
	@echo ""
	docker push pennsieve/rehydrate:${IMAGE_TAG}
