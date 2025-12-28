# AWS Distributed NLP System

A scalable, fault-tolerant distributed system for analyzing text files using Stanford NLP Parser. Built on AWS infrastructure (EC2, S3, SQS) with a Manager-Worker architecture.

![Architecture](https://img.shields.io/badge/Architecture-Manager--Worker-blue)
![AWS](https://img.shields.io/badge/AWS-EC2%20%7C%20S3%20%7C%20SQS-orange)
![Java](https://img.shields.io/badge/Java-17-red)
![NLP](https://img.shields.io/badge/NLP-Stanford%20Parser-green)

---

## 🚀 Features

- **Fully Scalable**: Dynamically scales workers based on workload - handles millions of concurrent requests
- **Fault Tolerant**: Automatic worker replacement, message retry, and crash detection
- **Multi-Client Support**: Processes multiple concurrent client requests simultaneously
- **Three NLP Analysis Types**: 
  - Part-of-Speech (POS) tagging
  - Constituency parsing
  - Dependency parsing

---

## 📋 Table of Contents

1. [System Architecture](#-system-architecture)
2. [Quick Start](#-quick-start)
3. [Usage](#-usage)
4. [Implementation Details](#-implementation-details)
5. [Fault Tolerance](#-fault-tolerance)
6. [Threading Model](#-threading-model)
7. [Scalability](#-scalability)

---

## 🏗 System Architecture

### Components

#### **LocalApplication** (Client)
- Uploads input file to S3
- Starts Manager if not already running
- Creates unique response queue per client
- Waits for completion and downloads results
- Handles Manager crash detection

#### **Manager** (Coordinator)
- Single EC2 instance coordinating all work
- Receives tasks from multiple clients concurrently
- Splits tasks into subtasks (one per URL+analysis pair)
- **Dynamic worker scaling**: Launches workers based on `n` ratio
- Collects results and generates HTML summaries
- **Automatic worker replacement**: Monitors worker health every 60s

#### **Worker** (Processing Nodes)
- Multiple EC2 instances processing tasks in parallel
- Single-threaded design (horizontal scaling, not vertical)
- Downloads text from URLs
- Performs CPU-intensive Stanford NLP parsing
- Uploads results to S3

### Communication Flow

```
┌─────────────────┐
│ LocalApp Client │
└────────┬────────┘
         │ Upload input → S3
         │ Send "new task"
         ▼
┌─────────────────────────────────────┐
│      manager-input-queue (SQS)     │
└────────┬────────────────────────────┘
         │ Manager polls
         ▼
┌─────────────────────────────────────┐
│            MANAGER                  │
│  • Downloads input from S3          │
│  • Creates subtasks                 │
│  • Launches workers (dynamic)       │
└────────┬────────────────────────────┘
         │ Enqueue subtasks
         ▼
┌─────────────────────────────────────┐
│      worker-tasks-queue (SQS)      │  ← Shared by all workers
└────────┬────────────────────────────┘
         │ Workers poll (distributed)
         ▼
┌─────────────────────────────────────┐
│       WORKERS (1 to 1000s)          │
│  • Download text from URL           │
│  • Perform NLP analysis (CPU)       │
│  • Upload result to S3              │
└────────┬────────────────────────────┘
         │ Send completion
         ▼
┌─────────────────────────────────────┐
│     worker-results-queue (SQS)     │
└────────┬────────────────────────────┘
         │ Manager polls
         ▼
┌─────────────────────────────────────┐
│            MANAGER                  │
│  • Collects results                 │
│  • Generates HTML summary           │
│  • Uploads summary to S3            │
└────────┬────────────────────────────┘
         │ Send "DONE"
         ▼
┌─────────────────────────────────────┐
│   response-{clientId}-queue (SQS)  │  ← Unique per client
└────────┬────────────────────────────┘
         │ Client polls
         ▼
┌─────────────────┐
│ LocalApp Client │
│ Downloads HTML  │
└─────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites

- **AWS Account** with appropriate permissions (EC2, S3, SQS)
- **Java 17+**
- **Maven 3.6+**
- **IAM Role** with EC2, S3, and SQS permissions

### Build

```bash
mvn clean package
```

This generates:
- `target/manager.jar` - Manager application
- `target/worker.jar` - Worker application
- `target/local-application.jar` - Client application

### AMI Setup (One-Time)

1. **Launch EC2 instance** (Amazon Linux 2023, t3.micro)
2. **Install Java 17**

3. **Upload JARs** to `/opt/dsp-app/` on the instance:
   ```bash
   sudo mkdir -p /opt/dsp-app
   sudo cp manager.jar worker.jar /opt/dsp-app/
   ```
4. **Create AMI** from the instance
5. **Update AMI ID** in `LocalApplication.java`:
   ```java
   private static final String AMI_ID = "ami-xxxxxxxxx"; // Your AMI ID
   ```
6. **Rebuild** LocalApplication:
   ```bash
   mvn clean package
   ```

---

## 💻 Usage

### Command

```bash
java -jar target/local-application.jar <inputFile> <outputFile> <n> [terminate]
```

### Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `inputFile` | Input file with URLs and analysis types | `input.txt` |
| `outputFile` | Output HTML file path | `output.html` |
| `n` | Files-per-worker ratio | `10` (1 worker per 10 URLs) |
| `terminate` | *(Optional)* Terminate Manager after completion | `terminate` |

### Input File Format

Each line: `<ANALYSIS_TYPE><TAB><URL>`

```
POS	https://www.gutenberg.org/files/1661/1661-0.txt
CONSTITUENCY	https://www.gutenberg.org/files/1342/1342-0.txt
DEPENDENCY	https://example.com/sample.txt
```

**Analysis Types:**
- `POS` - Part-of-Speech tagging
- `CONSTITUENCY` - Constituency parsing (tree structure)
- `DEPENDENCY` - Dependency parsing (word relationships)

### Output File Format

HTML with clickable links to input and result files:

```html
<html><body>
POS: <a href="https://input-url.com/file.txt">https://input-url.com/file.txt</a> 
     <a href="https://s3.amazonaws.com/bucket/result.txt">https://s3.amazonaws.com/bucket/result.txt</a><br>
CONSTITUENCY: <a href="...">...</a> <a href="...">...</a><br>
</body></html>
```

**Error handling:**
```html
POS: <a href="https://bad-url.com/file.txt">https://bad-url.com/file.txt</a> ERROR: Connection timeout<br>
```

### Example

```bash
# Start system with 1 worker per 5 files
java -jar target/local-application.jar input.txt output.html 5

# With automatic termination
java -jar target/local-application.jar input.txt output.html 10 terminate
```

---

## 🔧 Implementation Details

### AWS Resources

**SQS Queues (4 total):**

| Queue | Direction | Purpose | Message Format |
|-------|-----------|---------|----------------|
| `manager-input-queue` | LocalApp → Manager | Task submissions | `new task\t{s3_key}\t{n}\t{response_queue}` |
| `worker-tasks-queue` | Manager → Workers | Subtasks (shared) | `{taskId}\t{analysisType}\t{fileUrl}` |
| `worker-results-queue` | Workers → Manager | Completion messages | `{taskId}\t{analysisType}\t{url}\t{result}` |
| `response-{uuid}-queue` | Manager → LocalApp | Final results (unique per client) | `DONE\t{summary_s3_key}` |

**EC2 Configuration:**
- **Manager**: t3.micro instance
- **Workers**: t3.micro instances (dynamically scaled)
- **Region**: us-east-1
- **AMI**: Custom AMI with Java 17 + JARs

**S3 Storage:**
- Input files uploaded by clients
- Result files (POS/CONSTITUENCY/DEPENDENCY output) uploaded by workers
- HTML summary files generated by Manager

**SQS Settings:**
- Long polling: 20 seconds (reduces API costs)
- Visibility timeout: 4000 seconds (~66 minutes)
- Message retention: 4 days

### Security

**IAM Role-Based Authentication** (Zero credentials in code):
- All instances use `LabInstanceProfile` IAM role
- Temporary credentials auto-rotated by AWS
- No hardcoded access keys anywhere
- Prevents credential leakage

```java
// SDK automatically uses IAM instance profile
S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
```

---

## 🛡 Fault Tolerance

### Worker Crashes
- **Detection**: Manager health checks every 60 seconds via EC2 API
- **Recovery**: Automatic worker replacement when tasks are active
- **Message Safety**: SQS visibility timeout (66 min) ensures messages reappear if worker dies
- **Result**: Zero data loss, automatic recovery

### Manager Crashes
- **Detection**: LocalApp polls Manager status every 2 minutes
- **Response**: Throws exception with cleanup instructions
- **Limitation**: In-memory state lost (design choice for assignment scope)

### Worker Stalls
- **Detection**: Messages reappear after 66-minute visibility timeout
- **Recovery**: Stalled workers detected on next health check, messages re-processed
- **Prevention**: Workers deleted from queue only after successful completion

### Termination Process

Clean shutdown when client sends `terminate` flag:

1. Manager stops accepting new tasks
2. Manager waits for all active tasks to complete
3. Manager terminates all worker instances
4. Manager deletes system queues
5. Manager self-terminates
6. LocalApp deletes its response queue

**Result**: Zero orphaned resources

---

## 🧵 Threading Model

### Manager (Multi-threaded)

```
Main Thread
├─ Monitors shutdown condition
└─ Performs worker health checks (every 60s)

ClientListener Thread
├─ Polls manager-input-queue
├─ Submits tasks to executor (non-blocking)
└─ Handles new task requests

WorkerResultListener Thread
├─ Polls worker-results-queue
├─ Submits result processing to executor (non-blocking)
└─ Collects worker outputs

Executor Pool (CachedThreadPool - unlimited threads)
├─ Processes client messages concurrently
├─ Processes worker results concurrently
└─ Scales dynamically based on load
```

**Why multi-threaded?**
- **Concurrent client handling**: Process multiple clients simultaneously
- **Non-blocking listeners**: Queue polling never blocks message processing
- **Parallel result aggregation**: Collect results from many workers at once

### Workers (Single-threaded)

```
Main Thread
└─ while(true):
    ├─ Poll worker-tasks-queue (blocking, 20s)
    ├─ Download text from URL
    ├─ Perform NLP parsing (CPU-intensive)
    ├─ Upload result to S3
    ├─ Send completion message
    └─ Delete SQS message (ACK)
```

**Why single-threaded?**
1. **CPU-bound workload**: Stanford NLP parsing is 95%+ CPU time
2. **Horizontal scaling**: Add more workers instead of more threads per worker
3. **Simplicity**: No thread synchronization complexity
4. **Fair distribution**: SQS automatically distributes work across workers

---

## 📈 Scalability

### Worker Scaling Algorithm

When Manager receives a new task:

1. **Parse input file** and count total URLs
2. **Calculate required workers**: 
   ```
   required_workers = ceil(total_urls / n)
   ```
3. **Determine workers to launch**:
   ```
   workers_to_launch = max(0, required_workers - active_workers)
   ```
4. **Launch EC2 instances** with Worker JAR in user data


### Multi-Client Scaling

- Each client creates a **unique response queue** (`response-{uuid}-queue`)
- Manager processes **all client tasks concurrently**
- Workers pull from **shared task queue** (SQS load balancing)
- **No starvation**: Each client's messages are independent

### Theoretical Limits

| Scenario | System Response |
|----------|-----------------|
| 1 million clients | ✅ Scale to ~100,000 workers (AWS quota dependent) |
| 1 billion tasks | ✅ Queue unbounded, workers scale horizontally |
| Manager bottleneck | ⚠️ Single Manager limits throughput (~1000 reqs/sec) |

**Production optimization**: Use multiple Managers with load balancer or sharded task queues.

---

## 🛠 Technologies Used

- **Java 17** - Application runtime
- **Maven** - Dependency management
- **AWS EC2** - Compute instances
- **AWS S3** - Object storage
- **AWS SQS** - Message queuing
- **Stanford NLP Parser 3.6.0** - Natural language processing

---

## 📝 License

This project was developed as part of a Distributed Systems Programming course.

---

## 👥 Authors

**Ben Kapon** and **Ori Cohen**

---

