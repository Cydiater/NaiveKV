#!/bin/bash
set -e
set -o xtrace

mkdir -p ../build
cd ../build || exit 1
cmake -DCMAKE_BUILD_TYPE=Debug ..; make -j

cd unittest
arr=(unittest*)
cd ..
for test in "${arr[@]}"; do
    echo "Running ${test}"
    bash -c "./unittest/${test}"
done

cd ../script || exit 1


