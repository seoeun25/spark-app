spark-app
=========
Simple example of spark application

Build
-----
To build, use maven.

	mvn clean package

Running
-----
Submit application to cluster(standalone or yarn)

	cd SPARK_HOME
	bin/spark-submit --master yarn-cluster --class com.nexr.spark.WordCount spark-app-1.0-SNAPSHOT.jar


