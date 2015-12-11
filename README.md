spark-app
=========
Simple example of spark application.

# Build

To build, use maven.

	$ mvn clean package -DskipTests

modules
* example-java
* example-scala

# Running

Run application locally on 4 cores

```
$ bin/spark-sumbit \
    --class com.nexr.spark.simpleapp.SparkPiApp \
    --master local[4] \
    /path/to/spark-app/example-scala/target/example-scala-0.9-SNAPSHOT.jar 100
```
Run application on cluster (standalone or yarn)

```
$ bin/spark-submit \
    --master yarn-cluster \
    --class com.nexr.spark.WordCount spark-app-1.0-SNAPSHOT.jar
```


