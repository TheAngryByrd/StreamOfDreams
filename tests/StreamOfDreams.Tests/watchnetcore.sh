#!/bin/sh
dotnet restore
POSTGRES_HOST=localhost \
    POSTGRES_USER=$(whoami) \
    POSTGRES_PASS=postgres \
    POSTGRES_DB=postgres \
    dotnet watch run -f "$1" --no-restore -c Release

# POSTGRES_HOST=localhost \
#     POSTGRES_USER=$(whoami) \
#     POSTGRES_PASS=postgres \
#     POSTGRES_DB=postgres \
#     dotnet watch mono -f "$1" --no-restore -c Release
