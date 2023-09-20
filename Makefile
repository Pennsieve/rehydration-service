.PHONY: help clean test package publish test-ci

LAMBDA_BUCKET ?= "pennsieve-cc-lambda-functions-use1"
WORKING_DIR   ?= "$(shell pwd)"
SERVICE_NAME  ?= "rehydration-service"
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
	cd $(WORKING_DIR)/fargate/rehydrate; \
		go get github.com/pennsieve/rehydration-service/rehydrate

test-ci:
	@echo ""

# Spin down active docker containers.
docker-clean:		
	@echo ""

package:
	@echo ""
	@echo "***********************"
	@echo "*   Building Rehydrate Trigger lambda   *"
	@echo "***********************"
	@echo ""
	cd $(WORKING_DIR)/lambda/service; \
  		env GOOS=linux GOARCH=arm64 go build -tags lambda.norpc -o $(WORKING_DIR)/lambda/bin/service/bootstrap; \
		cd $(WORKING_DIR)/lambda/bin/service/ ; \
			zip -r $(WORKING_DIR)/lambda/bin/service/$(SERVICE_PACKAGE_NAME) .
	@echo ""
	@echo "***********************"
	@echo "*   Building Fargate   *"
	@echo "***********************"
	@echo ""
	cd $(WORKING_DIR)/fargate/rehydrate; \
		docker build -t pennsieve/rehydrate:${IMAGE_TAG} . ;\
		docker push pennsieve/rehydrate:${IMAGE_TAG} ;\

publish:
	@make package
	@echo ""
	@echo "*************************"
	@echo "*   Publishing Trigger lambda   *"
	@echo "*************************"
	@echo ""
	aws s3 cp $(WORKING_DIR)/lambda/bin/service/$(SERVICE_PACKAGE_NAME) s3://$(LAMBDA_BUCKET)/$(SERVICE_NAME)/service/
	rm -rf $(WORKING_DIR)/lambda/bin/service/$(SERVICE_PACKAGE_NAME)
