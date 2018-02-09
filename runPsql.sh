#!/usr/bin/env bash

run()
{
    if [[ "$OS" != "Windows_NT" ]]
    then
        mono "$@"
    else
        "$@"
    fi
}

run packages/build/FAKE/tools/FAKE.exe ./runPsql.fsx  "$@"
