# Dimensional Data Modeling

## OLTP <> Master Data <> OLAP
- OLTP (Online Transaction Processing) -> a software engineer's realm
- OLAP (Online Analytical Processing) -> Data analysts' realm
- Master Data: the bridge between both, the shield to architectural chaos

## Cumulative Table Design
- Secret sauce to zero-shuffle historical analysis
- FULL OUTER JOIN + COALESCE power the technique
- Go-to for time series data processing

## Run-length Encoding
- [A, A, A, B, B, C, C, C, C, A] turns to [(A, 3), (B, 2), (C, 4), (A, 1)]
- Best for Low cardinality columns

## Idempotency & Slow Changing Dimensions

### Idempotency
- Idempotency: The ability of a data pipeline to produce the same results regardless of the date, time, or how many times you run it.
- Don't run INSERT without TRUNCATE, skipping partition sensors, leave out start and/or end date
- MERGE statements & Window Controls are best friends

### Slow Changing Dimensions (SDC)
- Attributes that drift over time (i.e., age, online browser, etc.) for which you can track change history when modeled with the right type.
- Type 1: tempting, but destroys historical accuracy
- Type 3: A trap. Loses critical history between values
- Type 2: Gold Standard, use `is_current` to help yourself with debugging

### Advantage of Enums
- Automatic validation against predefined values, catching data drift before it impacts downstream
- Direct value casting eliminates expensive validation joins and lookups
- Combine date and enum values for optimal query patterns and storage optimization
- One source of truth for valid values across your entire data ecosystem
- Your schema becomes self-documenting

# Fact Data Fundamentals

## Fact Data: Truths
- Atomic: cannot be broken down any further
- Scale 10-100x more than dimension data
- Once recorded, they can't change

## Raw Logs vs Fact: Interconnected but not the same thing

### Raw Logs
- Realm of Software Engineers
- Ugly, Messy Schemas
- Quick system data
- Short Retention
- Duplicates

### Facts
- Realm of data engineers
- Clean analytical data
- Quality Guarantees
- Longer Retention
- Business Values

## Normalization vs Denormalization

### Normalization
- Splits data across multiple tables
- Minimizes data redundancy
- Removes duplicates, increases data integrity

### Denormalization
- Combines data into a single table
- Faster Querying (use GROUP BY, not join)
- High storage cost, potential duplications

## Network Logs at Neflict explained when to model dimensions in the fact data.
- The data was so massive, joining on IP address to find Network type was impossible. Therefore, it made sense to move the log to the Fact table.

# Fact Data Models

## General Statement: every data point is either capturing a state or telling a story about an event

### Dimensions: System's memory. They capture snapshots of a state
- Outside aggregations: show up in GROUP BY queries (user_id, gender, scoring class)
- Wide cardinality range

### Facts: System's diary. They record events as they happen.
- Inside aggregations (SUM, AVG, COUNT)
- Can be aggregated and turned into dimensions (bucketing with CASE WHEN) to reduce cardinality
- Generally 'higher' dimensions

# Shuffle: A deep dive
- Data points randomly rearranged (#1 enemy of parallelism)
- `SELECT + FROM + WHERE` without WINDOW function
    - Infinitely scalable query
    - Instant & Cheap processing, no shuffle
- `GROUP BY`
    - Triggers shuffle (need to have all data from given id in one machine)
    - Bucketization: split the data into `n` buckets using a key with high cardinality. It pre-shuffles the data, doing all the modulus grouping when you write the data out in your computing solution. 
    - Reduce data volume to help!
- `JOIN`
    - Same as GROUP BY but done on both left and right tables
- `ORDER BY` (avoid use in distributed computing)
    - End of query: not parallelizable

# Spark Fundamentals
- Hive uses DISC only, Spark enables RAM! Storage agnostic.

## The Plan (Play)
- The transformations and actions (written in Python, Scala, or SQL). Using a lazy evaluation approach, Spark analyzes the entire processing chain before executing it, optimizing operations and minimizing unnecessary computations.

## The Driver (Coach)
- Serves as the command center. It translates the plan into execution stages, determines optimal parallelism levels, and orchestrates data shuffling strategies.
    - 10 different driver settings. Only tweak these 2 (if necessary):
        - `spark.driver.memory`
            - Memory driver has to process job (default set: ~2GB; up to 16GB)
            - Caches for memory increase
                - Complex jobs: many steps and/or plans
                - Jobs using `dataframe.collect()` (the data the job processes changes the plan) - bad practice!
        - `spark.driver.memory`

## The Executors (Players)
- The workers that handle the actual processing. Each machine typically runs 4-6 parallel tasks, managing their own block storage and memory. They execute partitioned data processing, handle replication for fault tolerance, and optimize memory usage to prevent disk spilling.
    - `spark.executor.memory`
        - Memory executors get (default set: ~2GB; up to 16GB)
        - Spill to disk issue when memory runs low can lead to slow or break the job
        - Watchout: cost of engineering time > extra cloud storage
        - Ideal memory setup? Test run at different levels (i.e., 2/4/6GB) for several days and choose the smaller one that runs all the time
    - `spark.executor.cores`
        - Number of tasks per machine (default set: 4; up to 6)
        - Parallelism & execution speed increases with the increase in tasks, but can cause out of memory issues (higher probability of skew)
    - `spark.executor.memoryOverheadFactor`
        - Extra memory executors need for complex jobs (handling many UDFs)
        - UDFs handle JOIN or UNION operations (memory intensive)
        - How much extra memory? ~10% of non-heap tasks

## Shuffle Short-Merge Join
- Performs shuffling and sorting of both datasets before merging
- Perfect for large datasets (>10GB) when memory isn't constrained.
- Performance issues if your data distribution is heavily skewed.
- Will not work when you get to 10 TB+ level.

## Broadcast Hash Join
- Broadcasts the smaller dataset to all executors, eliminating shuffle needs
- Ideal when one side is small (<10 GB) and your executors have sufficient memory
- If your broadcast side exceeds the threshold (default 10MB), or both datasets are large, you're out of luck.

## Bucket Join
- Uses pre-bucketed data to avoid shuffling during join operations. The efficiency champion
- Works beautifully when data is already bucketed by the join key and bucket sizes align
- Maintaining bucketed tables comes with overhead
- If you're under 1TB, bucketing may not be worth it

## Best Practices
- Enable with `spark.sql.adaptive.enabled = True` to avoid skew (in Spark 3+)
- For joins with known outliers, split processing
- Balance complexity of solution vs performance gain

# Advanced Spark
- When applications scale, data persistence becomes critical. Three different Spark approaches come into play:
    - Memory storage: delivers speed but faces RAM limitations.
    - Disk storage: trades performance for reliability.
    - Memory-and-disk approach: offers balance, but spills to disk when memory pressure builds.

## The language choice shapes both development and performance.
- PySpark brings Python's ecosystem but includes serialization costs.
- Scala offers native JVM performance and type safety but requires deeper technical expertise.

## The API choice determines development flexibility and production reliability.
- Spark SQL enables rapid analytics and exploration.
- DataFrames balance flexibility with performance.
- The Dataset API brings type safety and testing capabilities crucial for production systems.

### Spark SQL
- Ideal for multi-user collaboration (data analysts/scientists)
- Most flexible for rapid changes
- Lowest entry barrier
- Simple null returns for null encounters

### Spark DataFrame
- Ideal for hardened PySpark pipelines with minimal change requirements
- Enables code modularization
- Better testability with function separation
- Simple null returns for null encounters

### Spark Dataset
- Ideal for strong software engineering practices & enterprise solutions
- Enhanced schema handling
- Superior unit and integration testing capabilities
- Easy mock data generation
- Built-in type safety
- Explicit null handling

## Parquet: column file format, run-length encoding, benefits from parallel processing more efficient querying.
- **DO**: `sortWithinPartitions` (parallelizable operations, maintains good data distribution, sorts locally, cost-effective, no expensive shuffle operations)
- **DO NOT**: `Global.sort()`

## Shuffle Partitions
- Default: 200 partitions (shoot for 100 MB/partition)

# Data Quality
- Robust data pipelines follow 6 DQ principles:
    1. Discoverability: data is made accessible & searchable.
    2. Usability: sit on intuitive, self-explanatory structures.
    3. Integrity: enforce consistency through validation.
    4. Timeliness: deliver data when business needs it.
    5. Value: address direct and indirect impacts.
    6. Clarity: understand what data represents.

## WAP (Write-Audit-Publish)
- Pipeline writes to staging table first.
- Quality checks execute against staging data.
- Checks pass? Staging & Prod partitions exchange.
- Checks fail? Alerts trigger. Troubleshooting needed.

### Features:
- Staging & Production tables with identical schemas.
- Guarantees no unchecked data reaches production.
- Complex due to staging-to-production movement.
- Partition exchanges may cause pipeline delays.

## Signal:
- Data writes directly to production.
- Quality checks run on production data.
- A separate signal table tracks validation status.
- Downstream pipelines wait for signal before consuming.

### Features:
- Prone to meeting SLAs due to faster data landing.
- Simpler pattern with data landing in one place.
- Uses less compute and I/O resources.
- May not handle ad-hoc queries well.
- Less intuitive for downstream users.

- WAP prioritizes guaranteed quality control.
- Signal optimizes for speed & resource efficiency.

# Streaming vs Batch

## Streaming vs. Batch Pipelines

### Runtime Pattern:
- **Streaming**: Runs 24/7, continuous data flow.
- **Batch**: Runs periodically (e.g., every 4 hours), like web servers.

### Architecture:
- Streaming pipelines behave more like continuous processes, while batch pipelines are structured as Directed Acyclic Graphs (DAGs).

