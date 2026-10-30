#!/bin/sh
# Starts the ReatMetric UI with the groundlink SLE test configuration.
# Start the provider first: cargo run --example sle_spacecraft -- @RAF_PORT@ @FCLTU_PORT@
. "@ENV_SCRIPT@"
exec java --module-path="@REATMETRIC_HOME@/deps" \
    -Dderby.stream.error.file="@CONFIG_DIR@/derby.log" -Dreatmetric.core.config="@CONFIG_DIR@/configuration.xml" \
    --add-exports javafx.base/com.sun.javafx.event=org.controlsfx.controls \
    -m eu.dariolucia.reatmetric.ui/eu.dariolucia.reatmetric.ui.ReatmetricUI "$@"
