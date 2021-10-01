#!/bin/bash

USAGE="$0 profile vsn old_vsn package_path"
EXAMPLE="$0 emqx 4.3.8-b3bb6075 v4.3.2 /home/alice/relup_dubug/downloaded_packages"

if [[ $# -ne 4 ]]; then
    echo "$USAGE"
    echo "$EXAMPLE"
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
cp *.lux "$TEMPDIR/"
cp -r http_server "$TEMPDIR/http_server"

exec docker run \
    -v "$TEMPDIR:/relup_test" \
    -w "/relup_test" \
    -e REBAR_COLOR=none \
    -it savonarola/emqx-relup-env:4.3 \
        lux \
        --case_timeout infinity \
        --var PROFILE="$PROFILE" \
        --var PACKAGE_PATH="/relup_test/packages" \
        --var ONE_MORE_EMQX_PATH="/relup_test/one_more_emqx" \
        --var VSN="$VSN" \
        --var OLD_VSN="$OLD_VSN" \
        stop-start.lux

