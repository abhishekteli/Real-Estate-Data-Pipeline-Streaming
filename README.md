# Real-Estate-Data-Pipeline-Streaming

## Introduction and Business Requirement
The goal of this initiative is to harness real-time real estate data from Zillow's API, ensuring dynamic tracking and analysis to support agile decision-making. By implementing a streaming architecture, the system captures, processes, and stores live data, offering up-to-date insights for market trends and investment opportunities. This approach caters to the need for a responsive, data-driven framework that supports real-time analytics and strategic planning in the ever-evolving real estate market.

## System Architecture
Data Extraction (streamingBronze class)
The streamingBronze class serves as the entry point for real-time data extraction. It initializes a Kafka producer with appropriate configurations and subscribes to a specific topic to receive real estate data. The class fetches data from the Zillow API, paginating through results to capture comprehensive listings. Each page of data is serialized into JSON and pushed to a Kafka topic, enabling downstream processing in a decoupled, scalable manner.

## Data Transformation (streamingGold class)
Upon receiving data, the streamingGold class leverages Spark Structured Streaming to process real estate information in real-time. It consumes messages from the Kafka topic, deserializes JSON payloads, and applies a predefined schema for data normalization. Transformation logic enriches and refines the data, parsing complex fields, handling nulls, and deriving additional insights. The processed data is then structured for consistency and relevance, aligning with analytical objectives.

## Data Persistence
The persistence layer, integrated within the streamingGold class, focuses on storing the transformed data into a PostgreSQL database. It implements a foreachBatch write strategy, where each micro-batch of processed data is conditionally written to the database based on its load date, supporting both historical analysis and current data tracking. Error handling and transaction management ensure data integrity and consistency across streaming writes.

## Execution Flow
The execution begins with the streamingBronze class initiating data extraction and publishing to Kafka. Concurrently, streamingGold reads this stream, applying transformations and persisting the results to the database. This loop continues, governed by user inputs for different locations or termination requests, showcasing a responsive and iterative data processing pipeline that adapts to user needs and market changes.


<img width="1055" alt="Screenshot 2024-03-13 at 10 31 22 PM" src="https://github.com/abhishekteli/Real-Estate-Data-Pipeline-Streaming/assets/26431142/3387adab-7710-43bc-b144-714deb70498c">
