<h2 align="center"> 
	🚧 Music Catalog Data Lake 🚧
</h2>


<p align="center">
  <img alt="GitHub language count" src="https://img.shields.io/github/languages/count/rmendes1/music_data_lake?color=%2304D361">

 <img alt="Repository size" src="https://img.shields.io/github/repo-size/rmendes1/music_data_lake">
	
  
  <a href="https://github.com/rmendes1/house-rocket/commits/main">
    <img alt="GitHub last commit" src="https://img.shields.io/github/last-commit/rmendes1/music_data_lake">
  </a>

  <img alt="License" src="https://img.shields.io/badge/license-MIT-brightgreen">
</p>

## Description
The **Music Data Lake** is a project focused on collecting and processing music data. It leverages technologies such as Apache Spark, Docker, and Debezium to handle large volumes of data and enable scalable analytics.

## Technologies Used
- **Apache Spark** - Distributed processing of large datasets.
- **Docker & Docker Compose** - Containerization for easy deployment.
- **Debezium** - Real-time data capture.
- **PostgreSQL** - Structured data storage.
- **Music API** - Data collection from external service (Deezer Client).

## Current Architecture
The project is still under development. But we can already see the road so far
<h1 align="center">
    <img alt="MusicDataImg" title="#MusicData" src="cdc_ingestion.drawio.png" />
</h1>

## Project Structure
```
/music_data_lake
│── api_consumer/        # Scripts for consuming music APIs
│── queries/             # SQL queries for data extraction and analysis
│── spark_files/         # Files and scripts for Spark processing
│── Dockerfile           # Docker container configuration
│── docker-compose.yml   # Service orchestration with Docker Compose
│── debezium.json        # Configuration for real-time data capture
```

## How to Run the Project
1. Clone the repository:
   ```bash
   git clone https://github.com/rmendes1/music_data_lake.git
   cd music_data_lake
   ```
2. Start the Docker containers:
   ```bash
   docker-compose up -d
   ```
3. Execute the data processing scripts as needed.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

