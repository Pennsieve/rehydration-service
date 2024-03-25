.PHONY: help go-get npm-install html-clean local-services test test-ci clean docker-clean package publish

LAMBDA_BUCKET ?= "pennsieve-cc-lambda-functions-use1"
WORKING_DIR   ?= "$(shell pwd)"
SERVICE_NAME  ?= "rehydration-service"
LAMBDA_BIN ?= $(WORKING_DIR)/lambda/bin
SERVICE_PACKAGE_NAME ?= "rehydration-service-${IMAGE_TAG}.zip"
MJML_DIR = message-templates/mjml
MJML_SRCS = $(wildcard $(MJML_DIR)/*.mjml)
HTML_DIR = rehydrate/shared/notification/html

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

# Run go mod tidy on modules
tidy:
	cd ${WORKING_DIR}/lambda/service; go mod tidy
	cd ${WORKING_DIR}/rehydrate/fargate; go mod tidy
	cd ${WORKING_DIR}/rehydrate/shared; go mod tidy

npm-install:
	npm install

$(HTML_DIR)/%.html: $(MJML_DIR)/%.mjml
		./node_modules/mjml/bin/mjml $< -o $@

email-templates: npm-install html-clean $(patsubst $(MJML_DIR)/%.mjml, $(HTML_DIR)/%.html, $(MJML_SRCS))

html-clean:
	rm -f $(HTML_DIR)/*

# Start the local versions of docker services
local-services: docker-clean
	docker-compose -f docker-compose.test-local.yaml down --remove-orphans
	docker-compose -f docker-compose.test-local.yaml up -d dynamodb-local minio-local

# Run tests locally
test: local-services email-templates
	./run-tests.sh test-common.env test-local.env
	docker-compose -f docker-compose.test-local.yaml down --remove-orphans

# Run dockerized tests (used on Jenkins)
test-ci: docker-clean email-templates
	docker-compose -f docker-compose.test-ci.yaml down --remove-orphans
	@IMAGE_TAG=$(IMAGE_TAG) docker-compose -f docker-compose.test-ci.yaml up --exit-code-from=tests-ci tests-ci

clean: docker-clean html-clean
	rm -fr $(LAMBDA_BIN)

# Spin down active docker containers.
docker-clean:
	docker-compose -f docker-compose.test-ci.yaml down --remove-orphans
	docker-compose -f docker-compose.test-local.yaml down --remove-orphans


package: email-templates
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

publish: package
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
