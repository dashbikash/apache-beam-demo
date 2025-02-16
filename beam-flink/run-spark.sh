#! /bin/sh
export SPARK_HOME=/home/bikash/.sdkman/candidates/spark/3.5.3
export SPARK_MASTER_HOST=localhost
cd $SPARK_HOME
./sbin/start-master.sh
./sbin/start-worker.sh spark://localhost:7077
