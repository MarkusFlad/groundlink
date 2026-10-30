#!/bin/sh
# Sets up ReatMetric (https://github.com/dariol83/reatmetric) as an SLE user for
# the groundlink example sle_spacecraft. See docs/reatmetric.md.
#
# Steps, each skipped if already done:
#   1. JDK 17 (Eclipse Temurin) and Maven 3.9.9 into the prefix, checksums verified
#   2. ReatMetric 1.1.2: clone, build, install the spacecraft UI deployment
#   3. the ReatMetric configuration for sle_spacecraft, with start scripts
#
# No root rights needed; nothing is written outside the prefix and the
# configuration folder.

set -eu

PREFIX="$HOME/opt"
CONFIG_DIR="$HOME/Reatmetric/sle_test"
HOST="127.0.0.1"
RAF_PORT=5200
FCLTU_PORT=5201
CONFIG_ONLY=false

REATMETRIC_VERSION=1.1.2
REATMETRIC_REPO=https://github.com/dariol83/reatmetric.git
MAVEN_VERSION=3.9.9

usage() {
    cat <<EOF
Usage: $0 [options]

  --prefix DIR       where JDK, Maven and ReatMetric go (default: $PREFIX)
  --config DIR       the ReatMetric configuration folder (default: $CONFIG_DIR)
  --host HOST        host of sle_spacecraft (default: $HOST)
  --raf-port PORT    RAF port of sle_spacecraft (default: $RAF_PORT)
  --fcltu-port PORT  FCLTU port of sle_spacecraft (default: $FCLTU_PORT)
  --config-only      only (re)write the configuration; JDK, Maven and
                     ReatMetric must be installed in the prefix already
  -h, --help         this help
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        --prefix) PREFIX="$2"; shift 2 ;;
        --config) CONFIG_DIR="$2"; shift 2 ;;
        --host) HOST="$2"; shift 2 ;;
        --raf-port) RAF_PORT="$2"; shift 2 ;;
        --fcltu-port) FCLTU_PORT="$2"; shift 2 ;;
        --config-only) CONFIG_ONLY=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$PREFIX"
PREFIX=$(cd "$PREFIX" && pwd)
ENV_SCRIPT="$PREFIX/reatmetric-env.sh"
REATMETRIC_HOME="$PREFIX/reatmetric-$REATMETRIC_VERSION"
DOWNLOADS="$PREFIX/downloads"

say() { printf '\n==> %s\n' "$*"; }
need() { command -v "$1" >/dev/null 2>&1 || { echo "missing tool: $1" >&2; exit 1; }; }

install_jdk() {
    if [ -x "$PREFIX/jdk17/bin/java" ]; then
        echo "JDK 17 already in $PREFIX/jdk17"
        return
    fi
    need curl; need python3; need sha256sum; need tar
    case "$(uname -m)" in
        x86_64) arch=x64 ;;
        aarch64|arm64) arch=aarch64 ;;
        *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;;
    esac
    mkdir -p "$DOWNLOADS"
    say "Downloading Eclipse Temurin JDK 17 ($arch)"
    curl -fsSL "https://api.adoptium.net/v3/assets/latest/17/hotspot?architecture=$arch&image_type=jdk&os=linux&vendor=eclipse" \
        -o "$DOWNLOADS/jdk17.json"
    set -- $(python3 -I -c 'import json,sys; p=json.load(open(sys.argv[1]))[0]["binary"]["package"]; print(p["link"], p["checksum"], p["name"])' "$DOWNLOADS/jdk17.json")
    curl -fsSL "$1" -o "$DOWNLOADS/$3"
    echo "$2  $DOWNLOADS/$3" | sha256sum -c -
    dir=$(tar tzf "$DOWNLOADS/$3" | head -1 | cut -d/ -f1)
    tar xzf "$DOWNLOADS/$3" -C "$PREFIX"
    ln -sfn "$dir" "$PREFIX/jdk17"
}

install_maven() {
    if [ -x "$PREFIX/maven/bin/mvn" ]; then
        echo "Maven already in $PREFIX/maven"
        return
    fi
    need curl; need sha512sum; need tar
    mkdir -p "$DOWNLOADS"
    say "Downloading Maven $MAVEN_VERSION"
    url="https://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz"
    curl -fsSL "$url" -o "$DOWNLOADS/maven.tar.gz"
    echo "$(curl -fsSL "$url.sha512" | cut -d' ' -f1)  $DOWNLOADS/maven.tar.gz" | sha512sum -c -
    tar xzf "$DOWNLOADS/maven.tar.gz" -C "$PREFIX"
    ln -sfn "apache-maven-$MAVEN_VERSION" "$PREFIX/maven"
}

write_env_script() {
    cat > "$ENV_SCRIPT" <<EOF
# JDK 17 and Maven for building and running ReatMetric: . $ENV_SCRIPT
export JAVA_HOME="$PREFIX/jdk17"
export PATH="\$JAVA_HOME/bin:$PREFIX/maven/bin:\$PATH"
EOF
}

install_reatmetric() {
    if [ -d "$REATMETRIC_HOME/deps" ]; then
        echo "ReatMetric $REATMETRIC_VERSION already in $REATMETRIC_HOME"
        return
    fi
    need git
    src="$PREFIX/src/reatmetric"
    if [ ! -d "$src/.git" ]; then
        say "Cloning ReatMetric"
        mkdir -p "$PREFIX/src"
        git clone -q "$REATMETRIC_REPO" "$src"
    fi
    git -C "$src" -c advice.detachedHead=false checkout -q "$REATMETRIC_VERSION"
    # shellcheck disable=SC1090
    . "$ENV_SCRIPT"
    say "Building ReatMetric $REATMETRIC_VERSION (takes a few minutes)"
    (cd "$src" && mvn -B -q -DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip=true install)
    (cd "$src" && mvn -B -q -DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip=true -f eu.dariolucia.reatmetric.ui.spacecraft/pom.xml package)
    mkdir -p "$REATMETRIC_HOME"
    cp -r "$src/eu.dariolucia.reatmetric.ui.spacecraft/target/deps" "$REATMETRIC_HOME/"
    ln -sfn "reatmetric-$REATMETRIC_VERSION" "$PREFIX/reatmetric"
}

install_config() {
    say "Writing the configuration to $CONFIG_DIR"
    if [ -e "$CONFIG_DIR" ]; then
        backup="$CONFIG_DIR.bak-$(date +%Y%m%d-%H%M%S)"
        mv "$CONFIG_DIR" "$backup"
        echo "previous configuration moved to $backup"
    fi
    mkdir -p "$CONFIG_DIR"
    CONFIG_DIR=$(cd "$CONFIG_DIR" && pwd)
    cp -r "$SCRIPT_DIR/config/." "$CONFIG_DIR/"
    find "$CONFIG_DIR" -type f | while read -r file; do
        sed -i \
            -e "s|@CONFIG_DIR@|$CONFIG_DIR|g" \
            -e "s|@ENV_SCRIPT@|$ENV_SCRIPT|g" \
            -e "s|@REATMETRIC_HOME@|$PREFIX/reatmetric|g" \
            -e "s|@RAF_ADDRESS@|$HOST:$RAF_PORT|g" \
            -e "s|@FCLTU_ADDRESS@|$HOST:$FCLTU_PORT|g" \
            -e "s|@RAF_PORT@|$RAF_PORT|g" \
            -e "s|@FCLTU_PORT@|$FCLTU_PORT|g" \
            "$file"
    done
    chmod +x "$CONFIG_DIR/start-ui.sh" "$CONFIG_DIR/check/check.sh"
}

if [ "$CONFIG_ONLY" = false ]; then
    install_jdk
    install_maven
    write_env_script
    install_reatmetric
elif [ ! -d "$PREFIX/reatmetric/deps" ] || [ ! -f "$ENV_SCRIPT" ]; then
    echo "ReatMetric is not installed in $PREFIX; run without --config-only first" >&2
    exit 1
fi
install_config

cat <<EOF

Done. Start the provider, then ReatMetric:

  cargo run --example sle_spacecraft -- $RAF_PORT $FCLTU_PORT
  $CONFIG_DIR/start-ui.sh          # with user interface
  $CONFIG_DIR/check/check.sh       # headless: sends TC(17,1), prints PASSED or FAILED
EOF
