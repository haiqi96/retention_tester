#!/usr/bin/env bash

# Error on undefined variable
set -u

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
repo_root="$script_dir/.."
package_path=$(readlink -f "$repo_root/build/clp-package")
if [ $? -ne 0 ]; then
  echo "Error: Failed to locate test package"
  exit 1
fi

python_path=$(readlink -f "$package_path/lib/python3/site-packages")
if [ $? -ne 0 ]; then
  echo "Error: Failed to locate package python lib"
  exit 1
fi

PYTHONPATH=$python_path \
    python3 \
    tester.py \
    "$package_path" \
    "$@"