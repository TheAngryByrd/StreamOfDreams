#!/bin/sh
dotnet restore
POSTGRES_HOST=localhost \
    POSTGRES_USER=$(whoami) \
    POSTGRES_PASS=postgres \
    POSTGRES_DB=postgres \
    dotnet watch run -f netcoreapp2.0 --no-restore -c Release

