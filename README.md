# rehydration-service

A serverless service responsible for recreating a previous version of a published dataset for download.

## Running tests

You will need Docker installed.

To run tests locally, run `make test`

To run or debug individual tests in your IDE:

* run `make local-services` to start the required service containers (`minio` and `dynamodb-local`)
* Use your IDE to create a run configuration that will set the environment variables found in `test-common.env`
  and `test-local.env`. (In IntelliJ you can create a configuration template for the project so that you only need
  to do this once.)
* run or debug the test

You can also run the tests in a Docker container by running `make test-ci`. This is what will be run by Jenkins.

Both `make test` and `make test-ci` run the script `run-tests.sh` to run tests. If you add a new module to this repo
you will need to update this script so that the tests are run automatically.

## Email Templates

This repo contains HTML email templates used when notifying users of completed rehydrations.

The source for the templates are MJML files located in `message-templates/mjml`.

To modify the templates you will need to:

* install `npm`
* make the changes to the source in `message-templates/mjml`
* run `make email-templates` to generate the HTML files (located in `rehydrate/shared/notification/html`)

