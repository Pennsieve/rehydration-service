.PHONY: help go-get local-services test test-ci clean docker-clean package publish

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

# Start the local versions of docker services
local-services:
	docker-compose -f docker-compose.test.yaml down --remove-orphans
	docker-compose -f docker-compose.test.yaml up -d dynamodb

# Run tests locally
test: local-services
	./run-tests.sh
	docker-compose -f docker-compose.test.yaml down --remove-orphans
	make clean


# Run dockerized tests (used on Jenkins)
test-ci:
	docker-compose -f docker-compose.test.yaml down --remove-orphans
	@IMAGE_TAG=$(IMAGE_TAG) docker-compose -f docker-compose.test.yaml up --exit-code-from=ci-tests ci-tests

clean: docker-clean
	rm -fr $(LAMBDA_BIN)

# Spin down active docker containers.
docker-clean:
	docker-compose -f docker-compose.test.yaml down

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
