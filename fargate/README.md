To build image:

`docker build -t pennsieve/rehydrate .`

To run container:

`docker run --env-file ./env.dev --name rehydrate pennsieve/rehydrate`