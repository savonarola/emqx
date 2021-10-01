#!/bin/bash

USAGE="$0 profile vsn old_vsn package_path"

if [[ $# -ne 4 ]]; then
    echo $USAGE
    exit 1
fi

set -ex

PROFILE="$1"
VSN="$2"
OLD_VSN="$3"
PACKAGE_PATH="$4"

TEMPDIR=$(mktemp -d)
trap '{ rm -rf -- "$TEMPDIR"; }' EXIT


git clone --branch=master "https://github.com/terry-xiaoyu/one_more_emqx.git" "$TEMPDIR/one_more_emqx"
cp -r "$PACKAGE_PATH" "$TEMPDIR/packages"
cp relup.lux "$TEMPDIR/relup.lux"
cp -r http_server "$TEMPDIR/http_server"

docker run \
    -v "$TEMPDIR:/relup_test" \
    -w "/relup_test" \
    -t savonarola/emqx-relup-env:4.3 \
        lux \
        -v \
        --case_timeout infinity \
        --var PROFILE="$PROFILE" \
        --var PACKAGE_PATH="/relup_test/packages" \
        --var ONE_MORE_EMQX_PATH="/relup_test/one_more_emqx" \
        --var VSN="$VSN" \
        --var OLD_VSN="$OLD_VSN" \
        relup.lux

