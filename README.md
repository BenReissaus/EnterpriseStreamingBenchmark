# Enterprise Streaming Benchmark

This repository contains the code that I developed as part of my master’s thesis “Design of a Benchmark Concept for Data Stream Management Systems (DSMS) in the Context of Smart Factories”. 

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


#### Benchmark Architecture 
![Benchmark Architecture](images/Architecture_Overview.jpg?raw=true)

#### Performance Metrics

1. Correctness
2. Response Time
3. Single Stream Throughput
4. Number of Streams


#### Benchmark Parameters
![Benchmark Dataflow](images/Benchmark_Dataflow.jpg?raw=true)

#### Toolkit
Datasender
Validator
System Scripts

### Implementation

- with Apache Flink for two queries 
- Identity Query
- Statistics Query: tumbling window of 1 second 





























