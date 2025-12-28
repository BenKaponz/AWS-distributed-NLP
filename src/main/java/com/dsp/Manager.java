package com.dsp;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Manager is the central coordinator of the distributed text analysis system.
 * It is responsible for:
 * - Receiving task requests from multiple LocalApplication clients via SQS
 * - Dynamically launching and managing Worker EC2 instances based on workload
 * - Distributing sub-tasks (individual URLs) to Workers
 * - Collecting results from Workers
 * - Generating HTML summaries and sending them back to clients
 * - Managing shutdown and cleanup of all Worker instances
 * 
 * The Manager supports multiple concurrent clients and ensures thread-safe operations
 * using concurrent data structures and atomic variables.
 * 
 * SCALABILITY:
 * - No hard limit on number of Workers - scales dynamically based on demand
 * - Supports millions of concurrent tasks and clients
 * - Only limited by AWS account quotas (which can be increased on request)
 */
public class Manager extends EC2Node {

    private static String AMI_ID;

    private static final String MANAGER_INPUT_QUEUE = "manager-input-queue";
    private static final String WORKER_TASKS_QUEUE = "worker-tasks-queue";
    private static final String WORKER_RESULTS_QUEUE = "worker-results-queue";

    private final Ec2Client ec2 = Ec2Client.builder().region(region).build();

    private final Map<String, TaskInfo> activeTasks = new ConcurrentHashMap<>();
    private final Map<String, Integer> workerRequirements = new ConcurrentHashMap<>();
    private final AtomicInteger activeWorkers = new AtomicInteger(0);
    private final Set<String> launchedWorkers = ConcurrentHashMap.newKeySet();

    private final ExecutorService executor = Executors.newCachedThreadPool();

    private final Set<String> activeClients = ConcurrentHashMap.newKeySet();

    private final AtomicBoolean acceptingNewTasks = new AtomicBoolean(true);

    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private Thread clientListener = null;
    private Thread workerResultListener = null;

    private String managerQueueUrl;
    private String workerTasksQueueUrl;
    private String workerResultsQueueUrl;

    /**
     * Main entry point for the Manager application.
     * 
     * @param args Command line arguments:
     *             args[0] - Worker AMI ID (required) - The AMI to use when launching Worker instances
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("[ERROR][MANAGER] AMI_ID argument required");
            System.err.println("[ERROR][MANAGER] Usage: java Manager <ami-id>");
            System.exit(1);
        }
        AMI_ID = args[0];
        System.out.println("[INFO][MANAGER] Manager starting with Worker AMI: " + AMI_ID);
        new Manager().run();
    }

    /**
     * Main execution loop of the Manager.
     * 
     * Initializes SQS queues, starts two listener threads:
     * 1. Client Listener - processes incoming task requests from LocalApplications
     * 2. Worker Result Listener - processes results from Workers
     * 
     * Monitors the system state and initiates shutdown when:
     * - A terminate signal has been received
     * - All tasks are completed
     * - No clients are waiting for results
     * 
     * After shutdown is triggered, waits for listener threads to complete,
     * terminates all Workers, and cleans up resources.
     */
    @Override
    public void run() {
        initialize();

        this.clientListener = new Thread(() -> listenForClientRequests(), "ClientListener");
        clientListener.start();

        this.workerResultListener = new Thread(() -> listenForWorkerResults(), "WorkerResultListener");
        workerResultListener.start();

        int counter = 0;
        while (!shouldTerminate()) {
            try {
                Thread.sleep(10000); 

                // Check worker health every 60 seconds (every 6 iterations)
                if (counter % 6 == 0 && !launchedWorkers.isEmpty()) {
                    checkAndReplaceDeadWorkers();
                }

                if (!acceptingNewTasks.get() && activeTasks.isEmpty() && activeClients.isEmpty()) {
                    System.out.println("[INFO][MANAGER] All tasks completed and no clients waiting. Initiating shutdown.");
                    shutdown.set(true);
                }

                if (!acceptingNewTasks.get()) {
                    if(counter % 6 == 0) 
                        System.out.println("[INFO][MANAGER] Shutdown in progress - Active tasks: "
                                + activeTasks.size() + ", Active clients: " + activeClients.size());
                }

            } catch (InterruptedException ignored) {
            }
            counter++;
        }

        if (workerResultListener != null)
            workerResultListener.interrupt();

        shutdownManager();

        try {
            clientListener.join(30000);
            workerResultListener.join(30000);
        } catch (InterruptedException ignored) {
        }
    }

    /**
     * Initializes the Manager by creating the required SQS queues:
     * - manager-input-queue: Receives task requests from LocalApplications
     * - worker-tasks-queue: Sends individual sub-tasks to Workers
     * - worker-results-queue: Receives results from Workers
     * 
     * If the queues already exist, retrieves their URLs.
     */
    private void initialize() {
        managerQueueUrl = sqs.createQueue(b -> b.queueName(MANAGER_INPUT_QUEUE)).queueUrl();
        workerTasksQueueUrl = sqs.createQueue(b -> b.queueName(WORKER_TASKS_QUEUE)).queueUrl();
        workerResultsQueueUrl = sqs.createQueue(b -> b.queueName(WORKER_RESULTS_QUEUE)).queueUrl();

        System.out.println("[INFO][MANAGER] Manager initialized.");
        System.out.println("[INFO][MANAGER] Manager queue: " + managerQueueUrl);
        System.out.println("[INFO][MANAGER] Worker tasks queue: " + workerTasksQueueUrl);
        System.out.println("[INFO][MANAGER] Worker results queue: " + workerResultsQueueUrl);
    }

    /**
     * Continuously polls the manager-input-queue for client requests.
     * Runs in a separate thread until shutdown is triggered.
     * 
     * Handles two types of messages:
     * 1. "terminate" - Sets acceptingNewTasks to false and breaks the loop
     * 2. Task requests - Format: "{client_id}\t{input_file_key}\t{n}"
     *                    Processes the request by downloading the input file,
     *                    creating sub-tasks, and launching Workers as needed
     * 
     * If a terminate signal has been received, new task requests are ignored.
     */
    private void listenForClientRequests() {
        while (!shouldTerminate()) {
            try {
                ReceiveMessageResponse response = receiveOne(managerQueueUrl);
                if (response.messages().isEmpty()) {
                    continue;
                }

                System.out.println("[INFO][MANAGER] Received message from client.");

                Message msg = response.messages().get(0);
                String body = msg.body();

                System.out.println("[INFO][MANAGER] Message body: " + body);

                if (body.equals("terminate")) {
                    acceptingNewTasks.set(false);
                    System.out.println("[INFO][MANAGER] Received terminate signal. No more tasks will be accepted.");
                    deleteMessage(managerQueueUrl, msg.receiptHandle());
                    break;

                } else if (acceptingNewTasks.get()) {
                    System.out.println("[INFO][MANAGER] Processing new task from client...");
                    executor.submit(() -> {
                        try {
                            handleClientMessage(body);
                        } catch (Exception e) {
                            System.err.println("[ERROR][MANAGER] Error handling client message: " + e.getMessage());
                            e.printStackTrace();
                        }
                    });

                    deleteMessage(managerQueueUrl, msg.receiptHandle());

                } else {
                    System.out.println("[INFO][MANAGER] Ignoring task message (shutdown in progress): " + body);
                    deleteMessage(managerQueueUrl, msg.receiptHandle());
                }

            } catch (Exception e) {
                System.err.println("[ERROR][MANAGER] Error in listenForClientRequests: " + e.getMessage());
                e.printStackTrace();
            }
        }

        System.out.println("[INFO][MANAGER] Client listener thread exiting.");
    }

    /**
     * Processes a task request message from a LocalApplication client.
     * 
     * Expected message format: "new task\t{input_file_key}\t{n}\t{response_queue_name}"
     * 
     * Actions:
     * 1. Downloads the input file from S3
     * 2. Parses each line to create sub-tasks (URL + analysis type)
     * 3. Creates a TaskInfo object to track the task
     * 4. Sends each sub-task to the worker-tasks-queue
     * 5. Calculates required number of Workers and launches them if needed
     * 6. Schedules periodic Worker scaling checks
     * 
     * @param msgBody The message body containing task request details
     */
    private void handleClientMessage(String msgBody) {
        String[] parts = msgBody.split("\t", 4);
        if (parts.length < 4) {
            System.err.println("[ERROR][MANAGER] Invalid message format: " + msgBody);
            return;
        }

        if (!parts[0].equals("new task")) {
            System.err.println("[ERROR][MANAGER] Unknown message type: " + parts[0]);
            return;
        }

        String inputKey = parts[1];
        int n = Integer.parseInt(parts[2]);
        String responseQueueName = parts[3];

        String taskId = UUID.randomUUID().toString();
        System.out.println("[INFO][MANAGER] Received new task: " + taskId + " from input file: " + inputKey);

        activeClients.add(responseQueueName);

        try {
            Path tempFile = Files.createTempFile("input-", ".txt");
            Files.delete(tempFile);

            try {
                s3.getObject(
                        GetObjectRequest.builder()
                                .bucket(bucketName)
                                .key(inputKey)
                                .build(),
                        tempFile);

                List<String> lines = Files.readAllLines(tempFile);
                List<String[]> subtasks = new ArrayList<>();

                for (String line : lines) {
                    if (!line.trim().isEmpty()) {
                        String[] components = line.split("\t", 2);
                        if (components.length == 2) {
                            subtasks.add(components);
                        }
                    }
                }

                if (subtasks.isEmpty()) {
                    System.err.println("[ERROR][MANAGER] No valid tasks in input file: " + inputKey);
                    activeClients.remove(responseQueueName);
                    String responseQueueUrl = sqs.getQueueUrl(b -> b.queueName(responseQueueName)).queueUrl();
                    sendMessage(responseQueueUrl, "ERROR\tNo valid tasks in input file");
                    return;
                }

                TaskInfo task = new TaskInfo(taskId, subtasks.size(), responseQueueName);
                activeTasks.put(taskId, task);
                workerRequirements.put(taskId,
                        Math.max(1, (subtasks.size() + n - 1) / n));

                enqueueWorkerSubtasks(taskId, subtasks);
                launchWorkersIfNeeded(taskId, workerRequirements.get(taskId));

            } finally {
                try {
                    Files.delete(tempFile);
                } catch (Exception ignored) {
                }
            }

        } catch (Exception e) {
            System.err.println("[ERROR][MANAGER] Error handling client message: " + e.getMessage());
            e.printStackTrace();
            activeClients.remove(responseQueueName);
            activeTasks.remove(taskId);
            workerRequirements.remove(taskId);
        }
    }

    /**
     * Launches new Worker EC2 instances if needed to meet the required number of Workers.
     * 
     * The system is fully scalable with no hard limit on the number of Workers.
     * The number of Workers to launch is calculated as: required - current
     * 
     * This ensures:
     * - The system scales dynamically based on workload
     * - Workers are only launched when needed
     * - No artificial cap on scalability (supports millions of concurrent tasks)
     * 
     * Each Worker is launched with:
     * - The specified Worker AMI
     * - t3.micro instance type
     * - User data script to start the Worker application
     * - IAM instance profile for AWS permissions
     * - "Role=Worker" tag for identification
     * 
     * This method is synchronized to prevent race conditions where multiple threads
     * might read the same worker count and launch duplicate workers.
     * 
     * @param taskId ID of the task requesting Workers (for logging purposes)
     * @param requiredWorkers Total number of Workers needed across all active tasks
     */
    private synchronized void launchWorkersIfNeeded(String taskId, int requiredWorkers) {
        int currentWorkers = activeWorkers.get();
        int workersToLaunch = Math.max(0, requiredWorkers - currentWorkers);

        if (workersToLaunch <= 0) {
            System.out.println("[INFO][MANAGER] No new workers needed. Current: "
                    + currentWorkers + ", Required: " + requiredWorkers);
            return;
        }

        System.out.println("[INFO][MANAGER] Launching " + workersToLaunch + " workers for task " + taskId);

        String userData = Base64.getEncoder().encodeToString(
                ("#!/bin/bash\ncd /opt/dsp-app\njava -cp worker.jar com.dsp.Worker\n").getBytes());

        int successfulLaunches = 0;

        for (int i = 0; i < workersToLaunch; i++) {
            try {
                RunInstancesRequest req = RunInstancesRequest.builder()
                        .imageId(AMI_ID)
                        .instanceType(InstanceType.T3_MICRO)
                        .minCount(1)
                        .maxCount(1)
                        .userData(userData)
                        .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                                .name("LabInstanceProfile").build())
                        .tagSpecifications(TagSpecification.builder()
                                .resourceType(ResourceType.INSTANCE)
                                .tags(Tag.builder().key("Role").value("Worker").build())
                                .build())
                        .build();

                RunInstancesResponse resp = ec2.runInstances(req);

                if (!resp.instances().isEmpty()) {
                    String instanceId = resp.instances().get(0).instanceId();
                    launchedWorkers.add(instanceId);
                    activeWorkers.incrementAndGet();
                    successfulLaunches++;
                    System.out.println("[INFO][MANAGER] Launched worker instance: " + instanceId);
                } else {
                    System.err.println("[ERROR][MANAGER] Failed to launch worker instance: empty response");
                }

            } catch (Exception e) {
                System.err.println("[ERROR][MANAGER] Error launching worker: " + e.getMessage());
            }
        }

        if (successfulLaunches > 0) {
            System.out.println("[INFO][MANAGER] Successfully launched " + successfulLaunches
                    + " out of " + workersToLaunch + " workers");
        }
    }

    /**
     * Checks the health of all launched Worker instances and replaces dead ones.
     * 
     * This method:
     * 1. Queries EC2 for the current status of all Worker instances
     * 2. Identifies terminated, stopped, or failed Workers
     * 3. Removes dead Workers from tracking (launchedWorkers set)
     * 4. Updates the activeWorkers counter to reflect actual running count
     * 5. If Workers have died and tasks are active, launches replacements
     * 
     * Called periodically (every 60 seconds) from the main monitoring loop.
     * Enables automatic recovery from Worker failures without manual intervention.
     * 
     * Fault tolerance mechanism:
     * - Dead Workers are detected via EC2 instance state
     * - activeWorkers count is corrected to match reality
     * - Next call to launchWorkersIfNeeded() will launch replacements
     * - Combined with SQS visibility timeout for message-level fault tolerance
     */
    private void checkAndReplaceDeadWorkers() {
        try {
            // Query EC2 for actual instance states
            DescribeInstancesResponse response = ec2.describeInstances(
                    DescribeInstancesRequest.builder()
                            .instanceIds(launchedWorkers)
                            .build());

            int aliveCount = 0;
            Set<String> deadWorkers = new HashSet<>();

            // Check each instance's state
            for (var reservation : response.reservations()) {
                for (var instance : reservation.instances()) {
                    String state = instance.state().nameAsString();
                    String instanceId = instance.instanceId();

                    if (state.equals("running") || state.equals("pending")) {
                        aliveCount++;
                    } else if (state.equals("terminated") || state.equals("stopped") || 
                               state.equals("stopping") || state.equals("shutting-down")) {
                        deadWorkers.add(instanceId);
                        System.out.println("[WARN][MANAGER] Dead worker detected: " + instanceId 
                                + " (state: " + state + ")");
                    }
                }
            }

            // Remove dead workers from tracking
            if (!deadWorkers.isEmpty()) {
                launchedWorkers.removeAll(deadWorkers);
                int previousCount = activeWorkers.get();
                activeWorkers.set(aliveCount);
                
                System.out.println("[INFO][MANAGER] Removed " + deadWorkers.size() 
                        + " dead workers. Active workers: " + previousCount + " -> " + aliveCount);

                // If we have active tasks and lost workers, try to replace them
                if (!activeTasks.isEmpty() && !workerRequirements.isEmpty()) {
                    int maxRequired = workerRequirements.values().stream()
                            .mapToInt(Integer::intValue)
                            .max()
                            .orElse(0);
                    
                    if (maxRequired > aliveCount) {
                        System.out.println("[INFO][MANAGER] Launching replacement workers. "
                                + "Required: " + maxRequired + ", Alive: " + aliveCount);
                        launchWorkersIfNeeded("HEALTH-CHECK-REPLACEMENT", maxRequired);
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("[ERROR][MANAGER] Error checking worker health: " + e.getMessage());
            // Don't crash the monitoring loop - just log and continue
        }
    }

    /**
     * Sends all sub-tasks for a given task to the worker-tasks-queue.
     * 
     * Each sub-task message format: "{taskId}\t{analysisType}\t{url}"
     * 
     * @param taskId Unique identifier for the parent task
     * @param subtasks List of sub-tasks, where each sub-task is [analysisType, url]
     */
    private void enqueueWorkerSubtasks(String taskId, List<String[]> subtasks) {
        for (String[] subtask : subtasks) {
            String message = taskId + "\t" + subtask[0] + "\t" + subtask[1];
            sendMessage(workerTasksQueueUrl, message);
        }
        System.out.println("[INFO][MANAGER] Enqueued " + subtasks.size()
                + " subtasks for task " + taskId);
    }

    /**
     * Continuously polls the worker-results-queue for results from Workers.
     * Runs in a separate thread until shutdown is triggered.
     * 
     * Expected message format: "{taskId}\t{analysisType}\t{inputUrl}\t{outputKey}"
     * 
     * Each result is processed asynchronously in the thread pool to avoid blocking
     * the listener thread.
     */
    private void listenForWorkerResults() {
        try {
            while (!shouldTerminate()) {
                var response = receiveOne(workerResultsQueueUrl);
                if (response.messages().isEmpty())
                    continue;

                var msg = response.messages().get(0);
                String body = msg.body();

                executor.submit(() -> {
                    try {
                        String[] parts = body.split("\t", 4);
                        if (parts.length >= 4) {
                            recordWorkerResult(parts[0], parts[1], parts[2], parts[3]);
                        }
                    } catch (Exception e) {
                        System.err.println("[ERROR][MANAGER] Error processing worker result: " + e.getMessage());
                    }
                });

                deleteMessage(workerResultsQueueUrl, msg.receiptHandle());
            }

        } catch (Exception e) {
            if (!shouldTerminate()) {
                System.err.println("[ERROR][MANAGER] Error in listenForWorkerResults: " + e.getMessage());
            }
        }

        System.out.println("[INFO][MANAGER] Worker result listener thread exiting.");
    }

    /**
     * Records a result from a Worker for a specific sub-task.
     * 
     * Adds the result to the TaskInfo and checks if all sub-tasks for the task are complete.
     * If complete, generates the HTML summary, uploads it to S3, sends a DONE message to the client,
     * and removes the task from activeTasks.
     * 
     * @param taskId ID of the parent task
     * @param analysisType Type of analysis performed (POS/CONST/DEP)
     * @param inputUrl The URL that was analyzed
     * @param outputKey S3 key where the result is stored
     */
    private void recordWorkerResult(String taskId, String analysisType, String inputUrl, String outputKey) {
        TaskInfo task = activeTasks.get(taskId);

        if (task != null) {
            synchronized (task) {
                task.addResult(analysisType, inputUrl, outputKey);
                System.out.println("[INFO][MANAGER] Recorded result for task " + taskId
                        + ": " + inputUrl + " -> " + outputKey);

                if (task.isComplete())
                    finalizeTask(task);
            }
        } else {
            System.err.println("[ERROR][MANAGER] Result received for unknown task "
                    + taskId + ". Ignoring: " + outputKey);
        }
    }

    /**
     * Completes a task after all sub-tasks have been processed.
     * 
     * Actions:
     * 1. Generates an HTML summary file containing all results
     * 2. Uploads the summary to S3
     * 3. Sends a DONE message to the client's response queue with the S3 key
     * 4. Removes the task from activeTasks and workerRequirements
     * 5. Removes the client from activeClients
     * 
     * @param task The TaskInfo object representing the completed task
     */
    private void finalizeTask(TaskInfo task) {
        System.out.println("[INFO][MANAGER] Finalizing task: " + task.taskId);

        try {
            String summaryKey = generateSummaryFile(task);

            try {
                String responseQueueUrl = sqs.getQueueUrl(
                        b -> b.queueName(task.responseQueue)).queueUrl();
                sendMessage(responseQueueUrl, "DONE\t" + summaryKey);
                System.out.println("[INFO][MANAGER] Task completion message sent to client: " + task.responseQueue);

            } catch (Exception e) {
                System.err.println("[ERROR][MANAGER] Error sending completion message to client "
                        + task.responseQueue + ": " + e.getMessage());
            }

            activeTasks.remove(task.taskId);
            workerRequirements.remove(task.taskId);
            activeClients.remove(task.responseQueue);

            System.out.println("[INFO][MANAGER] Task " + task.taskId
                    + " finalized with summary: " + summaryKey);

        } catch (Exception e) {
            System.err.println("[ERROR][MANAGER] Error finalizing task "
                    + task.taskId + ": " + e.getMessage());
            e.printStackTrace();

            activeTasks.remove(task.taskId);
            workerRequirements.remove(task.taskId);
            activeClients.remove(task.responseQueue);
        }
    }

    /**
     * Generates an HTML summary file for a completed task.
     * 
     * The HTML file contains:
     * - For each sub-task: analysis type, link to input URL, link to S3 result
     * 
     * Format for each line:
     * {analysisType}: <a href="{inputUrl}">{inputUrl}</a> <a href="{outputUrl}">{outputUrl}</a><br>
     * 
     * The summary is uploaded to S3 at "summary/{taskId}.html"
     * 
     * @param task The TaskInfo object containing all results
     * @return The S3 key where the summary file was uploaded
     */
    private String generateSummaryFile(TaskInfo task) {
        StringBuilder html = new StringBuilder();

        html.append("<html><body>\n");

        for (SubTaskResult result : task.getResults()) {
            String inputUrl = result.inputUrl;
            
            html.append(result.analysisType).append(": ")
                    .append("<a href=\"").append(inputUrl).append("\">").append(inputUrl).append("</a> ");
            
            // Check if this is an error message (starts with "ERROR:")
            if (result.outputKey.startsWith("ERROR:")) {
                // Display error message directly (no link)
                html.append(result.outputKey);
            } else {
                // Normal result - create link to S3 file
                String outputUrl = "https://s3.amazonaws.com/" + bucketName + "/" + result.outputKey;
                html.append("<a href=\"").append(outputUrl).append("\">").append(outputUrl).append("</a>");
            }
            
            html.append("<br>\n");
        }

        html.append("</body></html>");

        String summaryKey = "summary/" + task.taskId + ".html";

        try {
            s3.putObject(
                    software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(summaryKey)
                            .contentType("text/html")
                            .build(),
                    software.amazon.awssdk.core.sync.RequestBody.fromString(html.toString()));

        } catch (Exception e) {
            System.err.println("[ERROR][MANAGER] Error uploading summary file: " + e.getMessage());
        }

        return summaryKey;
    }

    /**
     * Checks if the Manager should terminate.
     * 
     * @return true if shutdown has been initiated, false otherwise
     */
    private boolean shouldTerminate() {
        return shutdown.get();
    }

    /**
     * Terminates all Worker EC2 instances that were launched by this Manager.
     * 
     * Sends a termination request for all instance IDs in the launchedWorkers set,
     * then clears the set and resets the activeWorkers counter.
     */
    private void terminateAllWorkers() {
        if (launchedWorkers.isEmpty()) {
            System.out.println("[INFO][MANAGER] No workers to terminate.");
            activeWorkers.set(0);
            return;
        }

        System.out.println("[INFO][MANAGER] Terminating " + launchedWorkers.size() + " workers...");
        System.out.println("[INFO][MANAGER] Worker instances: " + launchedWorkers);

        try {
            ec2.terminateInstances(TerminateInstancesRequest.builder()
                    .instanceIds(launchedWorkers).build());

            System.out.println("[INFO][MANAGER] Termination request sent to "
                    + launchedWorkers.size() + " workers");

            launchedWorkers.clear();
            activeWorkers.set(0);

        } catch (Exception e) {
            System.err.println("[ERROR][MANAGER] Error terminating workers: " + e.getMessage());
            e.printStackTrace();
            launchedWorkers.clear();
            activeWorkers.set(0);
        }
    }

    /**
     * Initiates the Manager shutdown process.
     * 
     * Steps:
     * 1. Waits for all active tasks to complete (up to 5 minutes timeout)
     * 2. Terminates all Worker instances
     * 3. Shuts down the thread pool executor
     * 4. Deletes the SQS queues created by Manager
     * 5. Closes the AWS clients (S3, SQS, EC2)
     * 
     * If tasks are still active after the timeout, they are abandoned and
     * the shutdown proceeds anyway.
     */
    private void shutdownManager() {
        System.out.println("[INFO][MANAGER] Shutting down Manager...");

        long timeout = System.currentTimeMillis() + 300000;
        while (!activeTasks.isEmpty() && System.currentTimeMillis() < timeout) {
            System.out.println("[INFO][MANAGER] Waiting for " + activeTasks.size() + " active tasks to complete...");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ignored) {
            }
        }

        if (!activeTasks.isEmpty()) {
            System.err.println("[ERROR][MANAGER] " + activeTasks.size()
                    + " tasks still active after timeout!");
        }

        terminateAllWorkers();

        System.out.println("[INFO][MANAGER] Shutting down executor service...");
        executor.shutdown();

        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                System.out.println("[INFO][MANAGER] Executor did not shut down cleanly. Forcing shutdown...");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }

        System.out.println("[INFO][MANAGER] Deleting SQS queues...");
        deleteQueues();

        System.out.println("[INFO][MANAGER] Closing AWS clients...");
        try {
            s3.close();
            sqs.close();
            ec2.close();
        } catch (Exception e) {
            System.err.println("[ERROR][MANAGER] Error closing AWS clients: " + e.getMessage());
        }

        System.out.println("[INFO][MANAGER] Manager logic shutdown complete.");

        try {
            String instanceId = software.amazon.awssdk.regions.internal.util.EC2MetadataUtils.getInstanceId();
            System.out.println("[INFO][MANAGER] Terminating Manager instance: " + instanceId);

            Ec2Client ec2Local = Ec2Client.builder().region(region).build();
            ec2Local.terminateInstances(TerminateInstancesRequest.builder()
                    .instanceIds(instanceId).build());
            ec2Local.close();

            System.out.println("[INFO][MANAGER] Termination request sent for Manager instance.");

        } catch (Exception e) {
            System.err.println("[ERROR][MANAGER] Failed to self-terminate Manager instance: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("[INFO][MANAGER] Manager shutdown complete.");
    }

    /**
     * Deletes the three SQS queues created by the Manager.
     * This cleanup ensures no orphaned queues remain after Manager termination.
     * 
     * Queues deleted:
     * - manager-input-queue
     * - worker-tasks-queue
     * - worker-results-queue
     */
    private void deleteQueues() {
        try {
            if (managerQueueUrl != null) {
                System.out.println("[INFO][MANAGER] Deleting manager-input-queue...");
                sqs.deleteQueue(b -> b.queueUrl(managerQueueUrl));
                System.out.println("[INFO][MANAGER] manager-input-queue deleted");
            }
        } catch (Exception e) {
            System.err.println("[ERROR][MANAGER] Failed to delete manager-input-queue: " + e.getMessage());
        }

        try {
            if (workerTasksQueueUrl != null) {
                System.out.println("[INFO][MANAGER] Deleting worker-tasks-queue...");
                sqs.deleteQueue(b -> b.queueUrl(workerTasksQueueUrl));
                System.out.println("[INFO][MANAGER] worker-tasks-queue deleted");
            }
        } catch (Exception e) {
            System.err.println("[ERROR][MANAGER] Failed to delete worker-tasks-queue: " + e.getMessage());
        }

        try {
            if (workerResultsQueueUrl != null) {
                System.out.println("[INFO][MANAGER] Deleting worker-results-queue...");
                sqs.deleteQueue(b -> b.queueUrl(workerResultsQueueUrl));
                System.out.println("[INFO][MANAGER] worker-results-queue deleted");
            }
        } catch (Exception e) {
            System.err.println("[ERROR][MANAGER] Failed to delete worker-results-queue: " + e.getMessage());
        }
    }

    /************************************************/
    /*              TaskInfo Class                  */
    /************************************************/

    /**
     * Represents a client task consisting of multiple sub-tasks.
     * Thread-safe for concurrent result collection.
     */
    private static class TaskInfo {
        final String taskId;
        final int totalSubtasks;
        final String responseQueue;
        final List<SubTaskResult> results = Collections.synchronizedList(new ArrayList<>());

        /**
         * Creates a new TaskInfo.
         * 
         * @param id Unique identifier for this task
         * @param total Total number of sub-tasks expected
         * @param resp Name of the SQS queue to send completion message to
         */
        TaskInfo(String id, int total, String resp) {
            this.taskId = id;
            this.totalSubtasks = total;
            this.responseQueue = resp;
        }

        /**
         * Adds a result from a completed sub-task.
         * 
         * @param type Analysis type (POS/CONST/DEP)
         * @param inputUrl The URL that was analyzed
         * @param outputKey S3 key where the result is stored
         */
        void addResult(String type, String inputUrl, String outputKey) {
            results.add(new SubTaskResult(type, inputUrl, outputKey));
        }

        /**
         * Checks if all sub-tasks have been completed.
         * 
         * @return true if the number of results equals or exceeds totalSubtasks
         */
        boolean isComplete() {
            return results.size() >= totalSubtasks;
        }

        /**
         * Returns a copy of all collected results.
         * 
         * @return A new list containing all SubTaskResult objects
         */
        List<SubTaskResult> getResults() {
            return new ArrayList<>(results);
        }
    }

    /************************************************/
    /*           SubTaskResult Class                */
    /************************************************/

    /**
     * Represents the result of a single sub-task (one URL + one analysis type).
     */
    private static class SubTaskResult {
        final String analysisType;
        final String inputUrl;
        final String outputKey;

        /**
         * Creates a new SubTaskResult.
         * 
         * @param t Analysis type (POS/CONST/DEP)
         * @param u Input URL that was analyzed
         * @param k S3 key where the result is stored
         */
        SubTaskResult(String t, String u, String k) {
            this.analysisType = t;
            this.inputUrl = u;
            this.outputKey = k;
        }
    }
}
