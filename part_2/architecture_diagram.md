```
graph TD
  A["Bid Requests - 200K QPS"]
  A --> B["Edge Gateways / API Gateway"]
  B --> C["Message Broker: Apache Kafka"]
  
  %% Real-Time Processing (Speed Layer)
  subgraph SpeedLayer[Real-Time Processing]
    C --> D["Stream Processing (Flink/Storm)"]
    D --> E["Cross-Device Mapping (Redis)"]
    E --> F["Bid Evaluation Microservices"]
    F --> G["Bid Response"]
    D --> H["Real-Time Analytics (Time-Series DB)"]
  end
  
  %% Data Lake for Batch Processing
  C --> I["Raw Bid Logs & Engagement Events"]
  I --> J["Data Lake (S3/GCS/HDFS)"]
  
  %% Batch Processing Layer
  subgraph BatchLayer[Batch Processing]
    J --> K["Batch Processing (Spark)"]
    K --> L["Data Warehouse / Data Lakehouse - Starrocks/Clickhouse"]
  end
  
  %% Serving Layer for Unified Analytics
  subgraph ServingLayer[Serving Layer]
    L --> M["Unified Campaign Metrics - Visualize using Lookr, Metabase"]
  end
  
  H --- M
```