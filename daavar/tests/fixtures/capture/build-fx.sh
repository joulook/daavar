#!/usr/bin/env bash
# S3 fixture build, brain03 only (DRIVER-IMPL-NOTES.md S3): the fixture programs tf_fx and fx.Fx and the
# a2a-task benchmark, against the pinned tools of plan/witness/XCHECK-PINS-RESULT.md.
# usage: build-fx.sh <staged daavar tree> <work dir>
set -euo pipefail
D=$1; W=$2
X=/local/mkhoshechin/tmp/xcheck-pins
export PATH=$HOME/jdk17/bin:$HOME/.cargo/bin:$PATH JAVA_HOME=$HOME/jdk17 CARGO_HOME=/local/mkhoshechin/cargo \
       GRADLE_USER_HOME=/local/mkhoshechin/gradle TMPDIR=$W/tmp CARGO_INCREMENTAL=0 CARGO_NET_OFFLINE=true
export TRACEFORGE_COMMIT=$(git -C $X/tf rev-parse HEAD) JMC_COMMIT=$(git -C $X/jmc rev-parse HEAD)
mkdir -p $W/tmp $W/fx-target $W/a2a-target $W/fx-classes $W/a2a-classes
echo "tf $TRACEFORGE_COMMIT dirty=$(git -C $X/tf status --porcelain | wc -l); jmc $JMC_COMMIT dirty=$(git -C $X/jmc status --porcelain | tr '\n' ' ')"
FX=$D/daavar/tests/fixtures/src/fx
# tf_fx: the lock is a2a-task's with the root package renamed; the first build may rewrite it (offline, same pins)
[ -f $FX/rust/Cargo.lock ] || sed 's/^name = "tf_a2a_task"$/name = "tf_fx"/' $D/benchmarks/a2a-task/rust/Cargo.lock > $FX/rust/Cargo.lock
(cd $FX/rust && CARGO_TARGET_DIR=$W/fx-target cargo build --release --offline -j 16)
(cd $FX/rust && CARGO_TARGET_DIR=$W/fx-target cargo build --release --offline --locked -j 16)
(cd $D/benchmarks/a2a-task/rust && CARGO_TARGET_DIR=$W/a2a-target cargo build --release --offline --locked -j 16)
CP=$(cat $X/jmc/cp.txt)
rm -rf $W/fx-classes/* $W/a2a-classes/*
javac -d $W/fx-classes -cp "$CP" $(find $FX/java -name '*.java')
javac -d $W/a2a-classes -cp "$CP" $(find $D/benchmarks/a2a-task/java -name '*.java' -not -path '*/out/*')
ls -la $W/fx-target/release/tf_fx $W/a2a-target/release/tf_a2a_task $X/tf/target/release/conf-join
md5sum $FX/rust/Cargo.lock $FX/rust/Cargo.toml $FX/rust/src/*.rs $FX/java/fx/Fx.java
