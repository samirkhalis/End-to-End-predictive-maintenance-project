# ENSAB Academic Project : Industrial Machine Monitoring and Predictive Maintenance System using Big Data Tools and Machine Learning

A comprehensive data pipeline and analytics system for industrial machine monitoring, predictive maintenance, and real-time data processing. The system includes real-time data ingestion, processing, storage, analytics, and a chatbot interface for user interaction.

![Project Architecture](.Architecture.png)

## Table of Contents
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Running the Project](#running-the-project)
- [Component Details](#component-details)
- [Data Warehouse & Visualization](#data-warehouse--visualization)
- [Machine Learning Models](#machine-learning-models)
- [Chatbot Integration](#chatbot-integration)
- [Contributing](#contributing)
- [License](#license)

## Features
- Real-time data ingestion from industrial machine sensors via API
- Automated data pipeline using Apache Airflow
- Stream processing with Apache Kafka and Apache Spark
- Data storage in Apache Cassandra
- Data warehousing with Snowflake
- Business intelligence with Tableau
- Machine learning models for predictive maintenance
- Interactive chatbot for user queries
- Web interface built with Streamlit

## Prerequisites
- Docker Desktop
- Python 3.8+
- Java 8+
- Docker Compose
- Git
- Snowflake Account (for data warehouse)
- Tableau Desktop/Online (for visualization)
- Databricks Account (for ML model deployment)

## Project Structure
```
project/
├── docker-compose.yml
├── requirements.txt
├── airflow/
│   └── dags/
│       └── kafka_stream.py
├── pyspark/
│   ├── spark_structured_stream.py
│   └── Populate_Snowflake_DW.py
|   └── Model_Training.py
├── ml_models/
│   ├── maintenance_model_label_encoder.pkl
│   └── maintenance_model_model.pkl
│   └── maintenance_model_preprocessor.pkl
├── EDA_Models_Evaluation/
│   ├── EDA.ipynb
│   └── ModelsComparison.pkl
│   └── PySpark-Notebook-Random-Forest-Model.ipynb
│   └── ...
├── app.py
│ 
└── architecture_images...
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/samirkhalis/end-to-end-predictive-maintenance-project.git
cd project-name
```

2. Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. Start Docker containers:
```bash
docker-compose up -d
```

## Running the Project

### 1. Start Data Pipeline
```bash
# Start Airflow webserver
docker-compose up airflow-webserver

# Run PySpark streaming job
python pyspark/spark_structured_stream.py
```

### 2. Launch Streamlit Interface
```bash
streamlit run streamlit/app.py
```

## Component Details

### Apache Airflow DAG
The DAG handles:
- API data extraction
- Data transformation
- Loading to Kafka
Check `/dags/kafka_stream.py` for implementation details.

### PySpark Scripts
- `spark_structured_stream.py`: Real-time data processing from Kafka to Cassandra
- `Populate_Snowflake_DW.py`: Batch processing for data warehouse loading
- `Model_Training.py`: Implementing Random Forest algorithm using sparkML

### ML Models
- Jupyter notebooks for model evaluation in `ml_models/`
- Databricks notebook implementation: [(https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3236080291072813/2569457119641555/1875109331708444/latest.html)]
- Final Random Forest model deployed in production

## Data Warehouse & Visualization
- Snowflake data warehouse implementation
- Tableau dashboards for real-time monitoring
- Connection details in documentation
- ![DataWarehouse](.DataWarehouse-Schema.png)
- ![Real Time Visualisation](.Tableau-Dashboard.png)

## Machine Learning Models
Models implemented:
- Random Forest
- Gradient Boosting
- Model evaluation metrics and comparison available in notebooks

## Chatbot Integration
- Fine-tuned language models for domain-specific queries
- Integrated with Streamlit interface
- Custom training data and model weights in `chatbot/finetuned_models/`

## Screenshots
![Interface Screenshot](.interface-1.png)
![Interface Screenshot](.interface-1-prediction.png)
![Chatbot Interface Screenshot](.interface-2.png)

## Contributing
1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License
This project is licensed under the MIT License - see the LICENSE file for details
