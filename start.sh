#!/usr/bin/env bash
cd "$(dirname "$0")/gateway" || exit 1
./bin/run.sh root/conf.yaml
