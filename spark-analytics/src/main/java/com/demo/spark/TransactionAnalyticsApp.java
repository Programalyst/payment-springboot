package com.demo.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class TransactionAnalyticsApp {
    public static void main(String[] args) {

        // 1. Initialize Spark Engine configured to run locally
        SparkSession spark = SparkSession.builder()
                .appName("VisaTransactionAnalyticsEngine")
                .master("local[*]") // Dynamically leverage all CPU cores on your Mac
                .config("spark.ui.enabled", "false") // Disable Spark UI due to conflict with Spring Boot
                .config("spark.driver.bindAddress", "127.0.0.1") // Force Spark to bind to local loopback interface
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate();

        // 2. Establish Structured Stream Connection to Local Kafka Container
        Dataset<Row> rawKafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "payment-topic")
                .option("kafka.group.id", "spark-payment-analytics-group") // Unique ID for this consumer group
                .option("startingOffsets", "earliest")
                .load(); // This terminates the configuration map cleanly

        // 3. Define the Explicit Schema matching your TransactionResponse Record
        StructType paymentSchema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("status", DataTypes.StringType, false)
                .add("amount", DataTypes.DoubleType, false)
                .add("processedBy", DataTypes.StringType, true);

        // 4. Transform raw binary bytes into Structured Columns
        Dataset<Row> structuredTransactions = rawKafkaStream
                // 4a. Cast raw Kafka value binary data to a String
                .selectExpr("CAST(value AS STRING) as json_string", "CAST(key AS STRING) as kafka_key")
                // 4b. Parse the string into columns matching our schema definition
                .select(functions.from_json(functions.col("json_string"), paymentSchema).as("data"), functions.col("kafka_key"))
                // 4c. Flatten columns out of the 'data' wrapper so they act as root rows
                .select("kafka_key", "data.*");

        // 5. Pipe the live updating table straight to your IntelliJ Console
        try {
            structuredTransactions.writeStream()
                    .format("console")
                    .outputMode("append") // Displays new messages as they land
                    .start()
                    .awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
