# CSV to Avro Spark Pipeline
## Features

- **Reads CSV files** with configurable delimiter and schema inference
- **Data validation**: null checks, type casting, and error-tolerant conversion (malformed values become null)
- **Custom schema mapping** via configuration
- **Date and timestamp parsing** with custom formats
- **Deduplication** based on a configurable key
- **Processing timestamp** added to each record
- **Partitioned Avro output** (e.g., by date)
- **Structured logging** and error reporting
- **Unit tests** for type casting and parsing

## Setup

1. **Clone the repository**
   ```bash
   git clone 
   cd csv-to-avro-spark


2. Configure your application

Edit src/main/resources/application.conf to set:
sourceDir: Input CSV directory (e.g., data/input)
destDir: Output Avro directory (e.g., data/output)
dedupKey: Field for deduplication (e.g., id)
 partitionCol: Partition column (e.g., createdDate)
schemaMapping: Column name → Spark data type
dateFormats/timestampFormats: Custom parsing patterns

Example:

app {
  sourceDir = "data/input"
  destDir = "data/output"
  delimiter = ","
  dedupKey = "id"
  partitionCol = "createdDate"
  schemaMapping = {
    "id" = "IntegerType"
    "name" = "StringType"
    "amount" = "DecimalType(10,2)"
    "isActive" = "BooleanType"
    "createdDate" = "DateType"
    "eventTime" = "TimestampType"
  }
  dateFormats = {
    "createdDate" = "yyyy-MM-dd"
  }
  timestampFormats = {
    "eventTime" = "yyyy-MM-dd HH:mm:ss"
  }
}

##Build
Build the fat JAR:
sbt clean assembly

##Run
With Spark submit:
spark-submit ^
  --packages org.apache.spark:spark-avro_2.13:4.1.1 ^
  --class com.example.CsvToAvroApp ^
  --conf "spark.driver.extraJavaOptions=--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/sun.util.calendar=ALL-UNNAMED" ^
  target/scala-2.13/csv-to-avro-spark-assembly-0.1.0.jar
  Adjust the Avro package version to match your Spark version if needed.

##Output
Avro files will be written to the output directory, partitioned by the specified column (e.g., createdDate=2024-01-01/part-00000-...avro).
A _SUCCESS file indicates successful completion.

##Testing
Run unit tests:
sbt test

##Proof of success
<<<<<<< HEAD
<img width="1071" height="355" alt="Screenshot 2026-02-25 141102" src="https://github.com/user-attachments/assets/75a7bcee-45ed-4698-b1ac-b8ba6a390513" />
<img width="1453" height="686" alt="Screenshot 2026-02-25 141157" src="https://github.com/user-attachments/assets/83f37f55-6b4f-4bf9-b22e-d4f9216ed5d9" />
<img width="623" height="360" alt="Screenshot 2026-02-25 141225" src="https://github.com/user-attachments/assets/1b1e6bbd-df1f-41e0-8405-9473df4ca49f" />

=======
<img width="1071" height="355" alt="Screenshot 2026-02-25 141102" src="https://github.com/user-attachments/assets/9bada32f-c89c-4a17-beb6-64fcba31dfe8" />
<img width="1453" height="686" alt="Screenshot 2026-02-25 141157" src="https://github.com/user-attachments/assets/47107b96-6e06-49e4-b905-a5344a8ee8be" />
<img width="623" height="360" alt="Screenshot 2026-02-25 141225" src="https://github.com/user-attachments/assets/4506bc3e-1582-41a7-ad3d-fb45a9e5c38a" />
>>>>>>> 813cdca (add project files)
