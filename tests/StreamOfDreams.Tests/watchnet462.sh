#!/bin/sh

dotnet restore

POSTGRES_HOST=localhost \
    POSTGRES_USER=$(whoami) \
    POSTGRES_PASS=postgres \
    POSTGRES_DB=postgres \
    dotnet watch mono -f net462 --no-restore -c Debug -mo="--debug" --loggerlevel Warn
