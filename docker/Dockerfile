# Spark 3.5.1 with Hadoop 3
FROM apache/spark:3.5.1

# Install pip & Python deps (delta-spark pins a compatible pyspark)
USER root
RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*

# Optional: set timezone/locale if you want
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Spark + Delta Lake configs (applied to spark-submit/shell)
# These ensure Delta SQL extensions are available by default.
ENV SPARK_EXTRA_CLASSPATH="" \
    SPARK_SQL_EXTENSIONS="io.delta.sql.DeltaSparkSessionExtension" \
    SPARK_SQL_CATALOG="org.apache.spark.sql.delta.catalog.DeltaCatalog"

# Install Python libs
COPY docker/requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Download Delta JAR for Spark 3.5 (Scala 2.12 build)
RUN curl -L https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar \
    -o /opt/spark/jars/delta-spark_2.12-3.1.0.jar

# Delta Spark JAR (Scala 2.12 for Spark 3.5.x)
RUN curl -L https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar \
    -o /opt/spark/jars/delta-spark_2.12-3.1.0.jar

# Delta Storage JAR (pure Java)
RUN curl -L https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar \
    -o /opt/spark/jars/delta-storage-3.1.0.jar

# Workdir inside container
WORKDIR /app

# Keep the container running by default
CMD ["sleep", "infinity"]