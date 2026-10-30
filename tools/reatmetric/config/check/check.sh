#!/bin/sh
# Headless check: binds RAF and CLTU, sends TC(17,1) and waits for its verification.
# Start the provider first: cargo run --example sle_spacecraft -- @RAF_PORT@ @FCLTU_PORT@
. "@ENV_SCRIPT@"
exec java --module-path="@REATMETRIC_HOME@/deps" --add-modules ALL-MODULE-PATH \
    -Dderby.stream.error.file="@CONFIG_DIR@/derby.log" -Dreatmetric.core.config="@CONFIG_DIR@/configuration.xml" \
    "$(dirname "$0")/SleCheck.java"
