#!/usr/bin/env sh

for env_file in "$@"; do
    if [ -f "$env_file" ]; then
        set -o allexport && . "$env_file" && set +o allexport
    else
        echo "environment file $env_file is missing"
        exit 1
    fi
done

root_dir=$(pwd)

exit_status=0
cd "$root_dir/rehydrate/shared"
echo "RUNNING rehydrate/shared TESTS"
go test -v ./...; exit_status=$((exit_status || $? ))

echo "RUNNING lambda/service TESTS"
cd "$root_dir/lambda/service"
go test -v ./...; exit_status=$((exit_status || $? ))

echo "RUNNING lambda/expiration TESTS"
cd "$root_dir/lambda/expiration"
go test -v ./...; exit_status=$((exit_status || $? ))

echo "RUNNING rehydrate/fargate TESTS"
cd "$root_dir/rehydrate/fargate"
go test -v ./...; exit_status=$((exit_status || $? ))

exit $exit_status
