## Rehydration service

To build image locally:

`docker build -t pennsieve/rehydrate .`

To run container:

`docker run --env-file ./env.dev --name rehydrate pennsieve/rehydrate`