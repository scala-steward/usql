#!/usr/bin/env bash
set -e
export CI_COMMIT_TAG=`git describe --tags`
echo "Publishing for $CI_COMMIT_TAG"
source ~/bin/java21.sh

read -p "Press enter to continue"

sbt publishSigned sonatypeBundleRelease


