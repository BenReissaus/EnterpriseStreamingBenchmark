# Enterprise Streaming Benchmark

This repository contains the code that I developed as part of my master’s thesis “Design of a Benchmark Concept for Data Stream Management Systems (DSMS) in the Context of Smart Factories”.
The following sections give a rough overview. In case you are interested in design decisions or more details on the queries and performance results, send me a message on [LinkedIn](https://www.linkedin.com/in/benjamin-reissaus/).

### 1. Introduction

#### Problem Statement:

“Currently, there does not exist a satisfying application benchmark for distributed DSMSs in the area of smart factories.” 

#### Contributions:
1. Definition of a set of queries to be executed by the System Under Test (SUT).
2. Design and setup of the benchmark architecture.
3. Definition of a set of benchmark metrics to evaluate the SUT’s performance.
4. Provision of a basic toolkit including a data sender, validator and
system setup scripts.
5. Provision of a prototypical reference implementation for a subset of
the queries.


### 2. Benchmark Architecture 
![Benchmark Architecture](images/Architecture_Overview.jpg?raw=true)

### 3. Performance Metrics

1. Correctness
2. Response Time (90th-percentile)
3. Single Stream Throughput (in records/s)
4. Number of Streams


### 4. Benchmark Parameters

1. Number of input data streams (scale factor)
2. Data frequency per data stream
3. Benchmark duration
4. Queries to be executed on each data stream 

These parameters should be set in `tools/commons/commons.conf`.


### 5. Modules

#### 5.1 tools/commons
Contains code that is used by multiple modules and the main benchmark parameters are set in `commons.conf`. 

#### 5.2 tools/datasender
Contains the datasender. Kafka-specific configurations can be done in `tools/datasender/datasender.conf`.

#### 5.3 tools/validator
Contains the validator. Makes use of [Akka Stream Kafka Library](http://doc.akka.io/docs/akka-stream-kafka/current/home.html). 

#### 5.4 tools/configuration
Contains setup and configuration scripts and a benchmark runner. All of them are defined with Ansible. 

#### 5.5 tools/util
Contains utility functions to create/delete/redistribute Kafka topics and to get current offsets in topics.

#### 5.6. implementation
Partial benchmark implementation with Apache Flink for Identity Query (incoming events are written as are) and Statistics Query (min, max, mean, sum, count for tumbling window of 1 second).

### 6. Workflow

In case of a single data stream, the data sender reads the data records from a provided file (e.g. taken from [here](http://debs.org/debs-2012-grand-challenge-manufacturing-equipment/)) and sends them according to the configured frequency to the Kafka input topic. 
The SUT consumes the records, runs the configured queries and writes the results to the Kafka output topics (one dedicated Kafka topic per query). 
Afterwards, the validator can read the same records from the input topics, create gold standard results and compare 
them to the results created by the SUT to check for correctness. Furthermore, based on the Kafka message timestamps, the 90th-percentile of response times is calculated.  

In case of multiple data streams the setup is similar. Each data stream has a dedicated Kafka input topic. The SUT is required to run all configured queries on all data streams and write them to the dedicated output topics. 
The setup is shown in the following image in which the Identity and Statistics Query are executed by the Data Stream Management System on each data stream. 

![Benchmark Dataflow](images/Benchmark_Dataflow.jpg?raw=true)



























