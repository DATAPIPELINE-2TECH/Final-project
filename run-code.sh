export SPARK_MAJOR_VERSION=2
spark-submit --class mr.MainRun --master yarn-client --conf spark.driver.extraJavaOptions=-Dconfig.file=./conf/application.conf --jars ./conf/config-1.2.1.jar --num-executors=4 --executor-memory 4G --driver-memory 4g --conf spark.yarn.executor.memoryOverhead=2048 --deploy-mode client job-compte-fe.jar "rec"


