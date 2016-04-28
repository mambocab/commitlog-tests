#!/usr/bin/env sh

# This is a scripted test of the correctness of commitlog archiving and restoring.

# This function does what it says on the tin.
check_for_sstables() {
  echo Here are the SSTables:
  GETSSTABLE_OUTPUT="`ccm node1 getsstables -k $KS_NAME`"
  echo $GETSSTABLE_OUTPUT
  if [ -z $GETSSTABLE_OUTPUT ] ; then
    echo GOOD: no SSTables
  else
    echo BAD: We got SSTables -- you need to reconfigure commitlog logic here.
  fi
}

# Here, we make multiple runs easier to find visually.
py "random.choice(['uh huh its TEST TIME',
                   'who is testing. omg no way',
                   'lollllll'])" | figlet -ct -f slant

# These are going to be set the same for all our test runs.
set -e
CLUSTER_NAME=commitlog-archive-test
KS_NAME=ks
TABLE_NAME=tab
SNAPSHOT_NAME=test-snapshot

# These are options we want to tweak manually.
COMMITLOG_DIR=''
COMMITLOG_FAILURE_POLICY=''
COMMITLOG_SYNC_PERIOD=''

# We may want to tweak commitlog size, but in general, we want to keep it big.
# This prevents C* from flushing when we don't want it to.
COMMITLOG_TOTAL_SPACE=16384

# So, first we'll clean up from our last test run and initialize a 1-node Cassandra cluster.
ccm remove $CLUSTER_NAME || true  # allow remove to fail
ccm create $CLUSTER_NAME -n 1 -v git:trunk

# Then, we'll configure (the only node in) the cluster we just set up.
if [ ! -z $COMMITLOG_DIR ] ; then
  echo Setting custom commitlog directory
  ccm node1 updateconf -y "commitlog_directory: $COMMITLOG_DIR"
fi
if [ ! -z $COMMITLOG_FAILURE_POLICY ] ; then
  echo Setting custom commitlog failure policy
  ccm node1 updateconf -y "commit_failure_policy: $COMMITLOG_FAILURE_POLICY"
fi
if [ ! -z $COMMITLOG_TOTAL_SPACE ] ; then
  echo Setting custom commitlog maximum size
  ccm node1 updateconf -y "commitlog_total_space_in_mb: $COMMITLOG_TOTAL_SPACE"
fi

# Now we start our cluster, create our schema, and generate a csv of our data...
ccm start --wait-for-binary-proto
./data_util.py generate -o data.csv --keyspace-name ks --table-name tab -n 1000000

# and write some dataaaaaa.
./data_util.py load data.csv --keyspace-name ks --table-name tab

# Having written the data, we'll wait until the commitlog syncs to disk.
echo Sleeping until commitlog sync
if [ -z $COMMITLOG_SYNC_PERIOD ] ; then
  SLEEP_FOR=10
  echo No custom commitlog sync period\; sleeping ${SLEEP_FOR}s
  sleep ${SLEEP_FOR}  # default commitlog sync period
else
  echo Sleeping ${COMMITLOG_SYNC_PERIOD}s \(custom commitlog sync period\)
  sleep $COMMITLOG_SYNC_PERIOD
fi

# Now, we archive the commitlog:
echo Taking snapshot
ccm node1 nodetool "snapshot $KS_NAME -t $SNAPSHOT_NAME"

# Here, we check that we didn't generate any sstables. If we did, then we need
# to do more to make sure that commitlog flush doesn't happen.
# check_for_sstables

# Then we'll shut down C*, and we're not gonna be nice about it.
ccm stop --not-gently

rm ~/.ccm/$CLUSTER_NAME/node1/commitlogs/*

ccm start --wait-for-binary-proto

# ./data_util.py validate_empty --keyspace-name ks --table-name tab

ccm stop --not-gently

for f in `find ~/.ccm/$CLUSTER_NAME/node1/data*/ks/tab-*/snapshots/$SNAPSHOT_NAME -type f` ; do
  echo Copying $f
  cp $f -r ~/.ccm/$CLUSTER_NAME/node1/data`basename $SNAPSHOT_NAME`
done

ccm start --wait-for-binary-proto

# Checking that the same data is still there.
./data_util.py validate_same data.csv --keyspace-name ks --table-name tab
echo Congrats! Made it all the way to the end.
