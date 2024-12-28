# Smart City Data Processing Platform 🌆

Welcome to the **Smart City Data Processing Platform**, a simulation designed to collect, process, and analyze real-time urban data streams. This platform integrates vehicle telemetry, GPS coordinates, traffic camera feeds, and weather data to enable informed decision-making for smart city management.

## Key Features 🚀

- **Real-Time Data Streaming**: Tracks vehicles, processes GPS data, integrates traffic cameras, and monitors weather conditions in real-time.
- **Scalable Architecture**: Leverages Apache Kafka and Apache Spark for distributed data streaming and processing.
- **Event-Driven Design**: Built using an event-driven approach to efficiently handle diverse data streams.
- **Cloud Storage**: Stores processed data in Amazon S3 in Parquet format for efficient querying and analytics.
- **Extensible Framework**: Modular and adaptable to support additional data sources and processing requirements.

## Technology Stack 🛠️

- **Apache Kafka**: For real-time data streaming.
- **Apache Spark**: For distributed, large-scale data processing.
- **Python**: The primary programming language.
- **AWS S3**: For scalable and reliable data storage.
- **Docker**: For containerized deployment.
- **Confluent Kafka Python Client**: For Kafka integration.
- **PySpark**: For Spark streaming and batch processing.

## Prerequisites 📋

1. **System Requirements**:
   - Python 3.x
   - Apache Kafka
   - Apache Spark
   - AWS Account with S3 access
2. **Dependencies**:
   - Install required Python packages listed in `requirements.txt`.

## Installation ⚙️

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/smart-city-project.git
   cd smart-city-project
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Configure environment variables:

   ```bash
   export AWS_ACCESS_KEY="your-aws-access-key"
   export AWS_SECRET_KEY="your-aws-secret-key"
   export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
   ```

4. Configure Kafka topics:

   ```bash
   kafka-topics --create --topic vehicle_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   kafka-topics --create --topic gps_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   kafka-topics --create --topic traffic_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   kafka-topics --create --topic weather_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

## Usage 💻

1. Start Kafka and Spark services:

   ```bash
   docker-compose up -d
   ```

2. Run the data producer:

   ```bash
   python main.py
   ```

3. Start the Spark streaming application:

   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0 spark-city.py
   ```

## Data Schemas 📊

### Vehicle Data
- **id**: Unique identifier
- **device_id**: Device identifier
- **timestamp**: Data collection time
- **location**: (latitude, longitude)
- **speed**: Vehicle speed
- **direction**: Travel direction
- **make**: Vehicle manufacturer
- **model**: Vehicle model
- **year**: Manufacturing year
- **fuel_type**: Type of fuel used

### GPS Data
- **id**: Unique identifier
- **device_id**: Device identifier
- **timestamp**: Data collection time
- **speed**: Vehicle speed
- **direction**: Travel direction
- **vehicle_type**: Type of vehicle

### Traffic Camera Data
- **id**: Unique identifier
- **device_id**: Device identifier
- **camera_id**: Camera identifier
- **location**: Camera location
- **timestamp**: Data collection time
- **snapshot**: Base64 encoded image data

### Weather Data
- **id**: Unique identifier
- **device_id**: Device identifier
- **location**: Data collection location
- **timestamp**: Data collection time
- **temperature**: Current temperature
- **weather_condition**: Current weather condition
- **precipitation**: Rainfall amount
- **humidity**: Air humidity percentage
- **wind_speed**: Wind speed
- **air_quality_index**: AQI score

## Project Structure 📁

```
smart-city-project/
│
├── main.py              # Data producer and simulator
├── spark-city.py        # Spark streaming application
├── config.py            # Configuration settings
├── requirements.txt     # Python dependencies
├── docker-compose.yml   # Docker orchestration file
├── Dockerfile           # Docker image configuration
└── README.md            # Project documentation
```

## Security 🔒

- **Environment Variables**: Sensitive data such as AWS credentials and Kafka bootstrap servers are managed using environment variables.
- **SSL/TLS Encryption**: Kafka communication is secured with SSL/TLS.
- **AWS Encryption**: Data stored in S3 is encrypted using server-side encryption (SSE).
- **Access Control**: IAM roles follow the principle of least privilege.

## Troubleshooting 🛠️

- Ensure Kafka and Spark services are running.
- Check environment variable configurations.
- Review Kafka topic creation and configuration.
- Debug logs from `main.py` and `spark-city.py` for errors.

## Contributing 🤝

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Commit your changes (`git commit -m 'Add feature'`).
4. Push to the branch (`git push origin feature-branch`).
5. Open a Pull Request.

## License 📝

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact 📧

For any questions or suggestions, feel free to reach out:

- **Email**: your-email@example.com
- **GitHub**: [yourusername](https://github.com/yourusername)

