#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

OUTPUT_DIR="build"

# 创建输出目录
mkdir -p "$OUTPUT_DIR"

# 定义目标平台
platforms=(
  "darwin/amd64"
  "linux/amd64"
)

echo "Building $ENTRY for multiple platforms..."

for platform in "${platforms[@]}"
do
  IFS="/" read -r GOOS GOARCH <<< "$platform"
  output_name="$OUTPUT_DIR/app-$GOOS-$GOARCH"

  if [ "$GOOS" = "windows" ]; then
    output_name="$output_name.exe"
  fi

  echo "-> $GOOS/$GOARCH: $output_name"
  GOOS=$GOOS GOARCH=$GOARCH go build -o "$output_name" "$ENTRY"
done

echo "Build complete. Files are in $OUTPUT_DIR/"
