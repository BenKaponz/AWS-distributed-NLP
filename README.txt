# Distributed Text Analysis System

**Authors:** Ben Kapon and Ori Cohen  
**IDs:** 206001547 and 314804741  
**Course:** Distributed Systems Programming  
**Assignment:** 1 - Text Analysis in the Cloud

---

## Table of Contents
1. [Overview](#overview)
2. [System Architecture](#system-architecture)
3. [Running the Application](#running-the-application)
4. [Implementation Details](#implementation-details)
5. [Mandatory Requirements](#mandatory-requirements-checklist)

---

## Overview

A distributed system for analyzing text files using Stanford NLP Parser. The system processes URLs containing text files, performs linguistic analysis (POS tagging, constituency parsing, or dependency parsing), and generates an HTML summary with links to results.

### Key Features
- **Fully Scalable**: No artificial limits on workers or clients
- **Fault Tolerant**: Handles worker crashes, network failures, and message loss
- **Multi-Client**: Processes multiple concurrent client requests simultaneously
- **Efficient**: Dynamic worker scaling based on workload

---

## System Architecture

### Components

#### 1. **LocalApplication** (Client)
- Runs on user's local machine
- Uploads input file to S3
- Starts Manager if not running
- Creates unique response queue per client
- Waits for completion and downloads results
- Optionally sends terminate signal

#### 2. **Manager** (Coordinator)
- Runs on EC2 instance (single instance)
- Receives tasks from multiple clients simultaneously
- Downloads input files from S3
- Creates subtasks for each URL+analysis pair
- Launches workers dynamically based on workload
- Collects results and generates HTML summaries
- Manages worker lifecycle (launch and terminate)

#### 3. **Worker** (Processing Nodes)
- Run on EC2 instances (multiple instances)
- **Single-threaded** - processes one task at a time
- Poll worker-tasks-queue for work
- Download text from URLs
- Perform NLP analysis (POS/CONSTITUENCY/DEPENDENCY)
- Upload results to S3
- Send completion messages to Manager

#### 4. **EC2Node** (Base Class)
- Abstract base class for Manager and Worker
- Provides common AWS functionality (S3, SQS clients)
- Implements shared queue operations

### Communication Flow

```
┌─────────────────┐
│ LocalApp Client │
└────────┬────────┘
         │ (1) Upload input file to S3
         │ (2) Send "new task" message
         ▼
┌─────────────────────────────────────┐
│         manager-input-queue         │
└────────┬────────────────────────────┘
         │ (3) Manager polls
         ▼
┌─────────────────────────────────────┐
│            MANAGER                  │
│  - Downloads input from S3          │
│  - Creates subtasks                 │
│  - Launches workers                 │
└────────┬────────────────────────────┘
         │ (4) Enqueue subtasks
         ▼
┌─────────────────────────────────────┐
│        worker-tasks-queue           │
└────────┬────────────────────────────┘
         │ (5) Workers poll
         ▼
┌─────────────────────────────────────┐
│         WORKERS (multiple)          │
│  - Download text from URL           │
│  - Perform NLP analysis             │
│  - Upload result to S3              │
└────────┬────────────────────────────┘
         │ (6) Send completion message
         ▼
┌─────────────────────────────────────┐
│       worker-results-queue          │
└────────┬────────────────────────────┘
         │ (7) Manager polls
         ▼
┌─────────────────────────────────────┐
│            MANAGER                  │
│  - Collects all results             │
│  - Generates HTML summary           │
│  - Uploads to S3                    │
└────────┬────────────────────────────┘
         │ (8) Send "DONE" message
         ▼
┌─────────────────────────────────────┐
│      response-{clientId}-queue      │
└────────┬────────────────────────────┘
         │ (9) Client polls
         ▼
┌─────────────────┐
│ LocalApp Client │
│ - Downloads HTML│
│ - Saves locally │
└─────────────────┘
```

---

## Running the Application

### Build

```bash
mvn clean package
```

This creates:
- `target/manager.jar` - Manager with all dependencies
- `target/worker.jar` - Worker with all dependencies
- `target/local-application.jar` - LocalApplication

### AMI Setup (One-Time Setup)

Before running the application, you need to create an AMI with the JARs:

1. **Launch an EC2 instance** (Amazon Linux 2, t3.micro) with Java 17 installed
2. **Upload the JARs** (`manager.jar` and `worker.jar`) to `/opt/dsp-app/` on the instance
3. **Create an AMI** from this instance via EC2 Console
4. **Update the AMI ID** in `LocalApplication.java`:
   ```java
   private static final String AMI_ID = "ami-0dedd979fbd966072";
   ```
5. **Rebuild** the LocalApplication: `mvn clean package`

### Usage

```bash
java -jar local-application.jar inputFile.txt outputFile.html n [terminate]
```

**Parameters:**
- `inputFile.txt`: Input file with URLs and analysis types
- `outputFile.html`: Output HTML summary file
- `n`: Files per worker ratio (e.g., 10 means 1 worker per 10 files)
- `terminate`: (Optional) Terminate manager after completion

### Input File Format

Each line: `ANALYSIS_TYPE<TAB>URL`

```
POS	https://www.gutenberg.org/files/1659/1659-0.txt
CONSTITUENCY	https://example.com/text2.txt
DEPENDENCY	https://example.com/text3.txt
```

### Output File Format

HTML file with links to results:

```html
POS: <a href="input_url">input_url</a> <a href="s3_result_url">s3_result_url</a><br>
CONSTITUENCY: <a href="input_url">input_url</a> <a href="s3_result_url">s3_result_url</a><br>
```

For errors:
```html
POS: <a href="input_url">input_url</a> ERROR: Connection timeout<br>
```

---

## Implementation Details

### SQS Queues

We use **4 queues** for clear separation of concerns:

1. **manager-input-queue**: LocalApp → Manager
   - Message format: `new task\t{s3_key}\t{n}\t{response_queue_name}`
   - Also receives: `terminate` message

2. **worker-tasks-queue**: Manager → Workers
   - Message format: `{taskId}\t{analysisType}\t{fileUrl}`
   - Shared by all workers (SQS handles distribution)

3. **worker-results-queue**: Workers → Manager
   - Message format: `{taskId}\t{analysisType}\t{inputUrl}\t{outputKey}`
   - Workers send completion messages here (success or error)

4. **response-{clientId}-queue**: Manager → LocalApp (unique per client)
   - Message format: `DONE\t{summary_s3_key}`
   - Deleted after client receives response


### AWS Resources

**EC2 Instances:**
- **Manager:** t3.micro
- **Workers:** t3.micro

**SQS Configuration:**
- Long polling: 20 seconds (reduces API calls)
- Visibility timeout: 4000 seconds (66 minutes)

---

## Mandatory Requirements Checklist

###  Project Configuration
- **AMI:** ami-0dedd979fbd966072 
- **Instance Type:** t3.micro (both Manager and Workers)
- **Region:** us-east-1
- **n Value Used:** 3 (3 file per worker)
- **Runtime on sample input**: 75 minutes

---

###  Security - No Plain Text Credentials

**Question: Did you think for more than 2 minutes about security? Do not send your credentials in plain text!**

**Answer: YES** - IAM roles exclusively, zero credentials in code

**Implementation:**
- All EC2 instances use IAM Instance Profile: `LabInstanceProfile`
- Temporary credentials auto-rotated by AWS
- No access keys in code/S3/config files
- Cannot be stolen from codebase

**Code Example:**
```java
// NO ACCESS KEYS IN CODE!
S3Client s3 = S3Client.builder().region(region).build();
// AWS SDK automatically uses IAM instance profile
```

---

###  Scalability - Millions of Clients

**Question: Will your program work properly when 1 million clients connected at the same time? How about 2 million? 1 billion?**

**Answer: YES** (with AWS quota increases)

**Worker Scaling Logic:**

When Manager receives a new task:
1. **Downloads the input file from S3**
2. **Creates an SQS message for each URL** in the input file together with the operation (POS/CONSTITUENCY/DEPENDENCY)
3. **Calculates required workers**: `m = ceil(number_of_messages / n)`
4. **Launches workers based on current state**:
   - If there are **0 active workers**: Launch `m` workers
   - If there are **k active workers** and new job requires **m workers**: Launch `m - k` new workers (if needed)
5. **Workers pull from shared queue**: Manager does NOT delegate messages to specific workers. All workers take messages from the same SQS queue (worker-tasks-queue). With 2n messages and 2 workers, one worker might process n+(n/2) messages while the other processes only n/2.

**Scaling Formula:**
```
Required Workers for Task = ceil(Total URLs in Task / n)
Total Workers to Launch = max(0, Required Workers - Currently Active Workers)
```

---

###  Persistence & Fault Tolerance

**Question: What about persistence? What if a node dies? What if a node stalls for a while? What about broken communications?**


**Comprehensive Fault Tolerance Summary:**

**1. Worker Crashes:**
- SQS visibility timeout (66 min)
- Message not deleted → reappears → another worker processes it
- No data loss

**2. Manager Crashes:**
- LocalApp checks Manager status every 2 minutes
- If crashed, exits with error and cleanup instructions
- Limitation: In-memory state lost (no DynamoDB)

**3. Worker Stalls:**
- Message reappears after 66 min

---

###  Threading Design

**Question: Threads in your application, when is it a good idea? When is it bad?**

**Answer: Carefully designed - Manager multi-threaded, Workers single-threaded**

**Manager Threading (Multi-threaded):**
- **Main Thread**: Monitors shutdown
- **ClientListener Thread**: Polls manager-input-queue, delegates to executor
- **WorkerResultListener Thread**: Polls worker-results-queue, delegates to executor
- **Executor Pool (CachedThreadPool)**: Processes messages concurrently

**Worker Threading (Single-threaded):**

**Decision:** Single-threaded workers that scale horizontally

**Reasoning:**
1. **CPU-Bound Workload**: Workers are CPU-bound because Stanford NLP parsing dominates runtime.
I/O represents a small fraction of the total processing time.
2. **True Distributed**: Horizontal scaling (industry standard) - launch more workers instead of more threads
3. **Fair Distribution**: SQS distributes work fairly, no worker starvation


**Question: Are all your workers working hard? Or some are slacking?**

Answer: Yes. Workers are always busy as long as tasks exist.

- NLP parsing is CPU-bound, so workers stay busy almost all the time  
- SQS feeds tasks to any available worker  
- A worker is “slacking” only when it finished its task and the queue is empty  

---

###  Multiple Clients Tested

**Question: Did you run more than one client at the same time?**

Answer: YES - tested with 3 concurrent clients

---

###  System Understanding

**Question: Do you understand how the system works?**

Answer: YES - complete flow documented

See [Communication Flow diagram](#communication-flow) above.

---

###  Termination Process

**Question: Did you manage the termination process?**

Answer: YES - complete cleanup in 7 steps

**When client sends "terminate" flag:**
1. Manager stops accepting new tasks (`acceptingNewTasks.set(false)`)
2. Manager waits for all active tasks to complete 
3. Manager terminates all Worker instances
4. Manager deletes system queues (manager-input, worker-tasks, worker-results)
5. Manager closes AWS clients (S3, SQS, EC2)
6. Manager self-terminates
7. LocalApp deletes its response queue

**Note:** Even if LocalApp doesn't send "terminate", when LocalApp closes (successfully or crashes), it deletes its own response queue in the `finally` block. This ensures no orphaned response queues.

**Result:** All resources cleaned up, no orphaned instances/queues

---

###  Manager Work Separation

**Question: Is your manager doing more work than he's supposed to? Did you mix their tasks?**

Answer: NO - clear role boundaries

| Component | Responsibilities |
|-----------|-----------------|
| **LocalApp** | Upload input, send requests, download results |
| **Manager** | Coordinate, split tasks, launch workers, collect results, generate HTML |
| **Worker** | Download text, perform NLP parsing, upload results |

**Key Principle:** Manager only *coordinates*. Heavy work (parsing) distributed to Workers.

---

###  Understanding "Distributed"

**Question: Are you sure you understand what distributed means? Is there anything awaiting another?**

Answer: YES - true distributed system with no unnecessary waiting

**Distributed Properties:**
-  **Asynchronous Communication**: All communication via message queues, no direct blocking calls between components
-  **Independent Processing**: Workers operate independently, no inter-worker coordination required
-  **Horizontal Scalability**: Add more workers for more throughput (scale out, not up)
-  **Fault Independence**: One worker failure doesn't affect others
-  **Location Transparency**: Components can be anywhere in the cloud

**No Unnecessary Blocking:**
- Manager delegates work immediately to thread pool (non-blocking listeners)
- Workers process tasks independently (no synchronization between workers)
- All communication is asynchronous via queues

**Only Necessary Waiting:**
- Client waits for results (required by workflow - synchronous from user perspective)
- Manager waits for all sub-results before generating final output (required for correctness)

---
