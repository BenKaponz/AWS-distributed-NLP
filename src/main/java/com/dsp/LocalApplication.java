package com.dsp;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.UUID;

/**
 * Initializes AWS service clients (S3, SQS, EC2) for the US-EAST-1 region.
 * Generates a unique client ID used to create a dedicated response queue.
 */
public class LocalApplication {
    private static final String _BUCKET_NAME = "dsp-task1-text-analyzer-ben-ori";
    private static final String _MANAGER_QUEUE = "manager-input-queue";
    private static final String _AMI_ID = "ami-0dedd979fbd966072";

    Region region = Region.US_EAST_1;

    private final S3Client _s3;
    private final SqsClient _sqs;
    private final Ec2Client _ec2;
    private final String _client_id;

    /**
     * Constructs a new LocalApplication instance.
     * Initializes AWS clients (S3, SQS, EC2) for the US-EAST-1 region and generates
     * a unique client ID for this application instance.
     */
    public LocalApplication() {
        System.out.println("[INFO][LOCAL APP] Initializing AWS clients...");
        this._s3 = S3Client.builder().region(region).build();
        this._sqs = SqsClient.builder().region(region).build();
        this._ec2 = Ec2Client.builder().region(region).build();
        this._client_id = UUID.randomUUID().toString();
        System.out.println("[INFO][LOCAL APP] Initialization complete.");
    }

    /**
     * Entry point for the local application.
     *
     * Expected arguments:
     * args[0] – path to input file
     * args[1] – path to output HTML file
     * args[2] – max URLs per worker (n)
     * args[3] – optional "terminate" flag
     */
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("[ERROR][LOCAL APP] Usage: java LocalApplication inputFile outputFile n [terminate]");
            return;
        }

        new LocalApplication().run(
                args[0], args[1], Integer.parseInt(args[2]),
                args.length > 3 && args[3].equals("terminate"));
    }

    /**
     * Orchestrates the complete workflow:
     * 1. Ensures the S3 bucket exists
     * 2. Ensures the Manager EC2 instance is running (starts it if needed)
     * 3. Uploads the input file to S3
     * 4. Creates a dedicated SQS response queue for this client
     * 5. Sends a "new task" message to the Manager
     * 6. Waits for a DONE message using long polling
     * 7. Downloads the final HTML summary to the local machine
     * 8. Optionally sends a termination request to the Manager
     * 9. Cleans up the response queue and closes AWS clients
     * 
     * @param input_file  Path to the input file containing URLs and analysis types
     * @param output_file Path where the HTML summary will be saved
     * @param n           Number of files a single worker should process
     * @param terminate   If true, sends a terminate signal to Manager after
     *                    completion
     */
    public void run(String input_file, String output_file, int n, boolean terminate) {
        String response_queue_url = null;
        String manager_url = null;

        try {
            System.out.println("[INFO][LOCAL APP] Checking bucket status...");
            create_bucket_if_not_exists();

            System.out.println("[INFO][LOCAL APP] Checking if Manager EC2 instance is already running...");
            if (!is_manager_running()) {
                System.out.println("[INFO][LOCAL APP] Manager not running. Launching new instance...");
                start_manager();
                System.out.println("[INFO][LOCAL APP] Waiting for Manager to boot...");
                Thread.sleep(30000);
            } else {
                System.out.println("[INFO][LOCAL APP] Manager instance already running.");
            }

            System.out.println("[INFO][LOCAL APP] Ensuring Manager queue exists...");
            manager_url = create_queue(_MANAGER_QUEUE);

            System.out.println("[INFO][LOCAL APP] Uploading input file to S3: " + input_file);
            String input_key = upload_file(input_file);

            String response_queue_name = "response-" + _client_id;
            System.out.println("[INFO][LOCAL APP] Creating response queue: " + response_queue_name);
            response_queue_url = create_queue(response_queue_name);

            String msg = "new task\t" +
                    input_key + "\t" +
                    n + "\t" +
                    response_queue_name;

            System.out.println("[INFO][LOCAL APP] Sending new task message to Manager...");
            send_message(manager_url, msg);

            System.out.println("[INFO][LOCAL APP] Waiting for task completion...");
            String summary_key = wait_for_completion(response_queue_url);

            System.out.println("[INFO][LOCAL APP] Downloading result summary...");
            download_file(summary_key, output_file);

        } catch (Exception e) {
            System.err.println("[ERROR][LOCAL APP] LocalApp exception: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Send terminate signal in finally block to ensure it's sent even if errors occur
            if (terminate && manager_url != null) {
                try {
                    System.out.println("[INFO][LOCAL APP] Sending terminate signal to Manager...");
                    send_message(manager_url, "terminate");
                    Thread.sleep(1000);
                } catch (Exception e) {
                    System.err.println("[ERROR][LOCAL APP] Failed to send terminate signal: " + e.getMessage());
                }
            }

            // Clean up response queue
            if (response_queue_url != null) {
                System.out.println("[INFO][LOCAL APP] Deleting response queue...");
                try {
                    _sqs.deleteQueue(DeleteQueueRequest.builder()
                            .queueUrl(response_queue_url)
                            .build());
                    System.out.println("[INFO][LOCAL APP] Response queue deleted");
                } catch (Exception ignored) {
                    System.out.println("[WARN][LOCAL APP] Failed to delete response queue (ignored)");
                }
            }

            System.out.println("[INFO][LOCAL APP] Shutting down AWS clients...");
            try {
                _s3.close();
                _sqs.close();
                _ec2.close();
                System.out.println("[INFO][LOCAL APP] Clients closed.");
            } catch (Exception ignored) {
                System.out.println("[WARN][LOCAL APP] Error closing AWS clients (ignored)");
            }
        }
    }

    /**
     * Checks whether the target S3 bucket exists.
     * If it does not exist, creates it.
     *
     * Uses headBucket() to verify existence.
     */
    private void create_bucket_if_not_exists() {
        try {
            _s3.headBucket(HeadBucketRequest.builder().bucket(_BUCKET_NAME).build());
            System.out.println("[INFO][LOCAL APP] Bucket already exists: " + _BUCKET_NAME);
        } catch (NoSuchBucketException e) {
            System.out.println("[INFO][LOCAL APP] Bucket does not exist. Creating bucket: " + _BUCKET_NAME);
            _s3.createBucket(CreateBucketRequest.builder().bucket(_BUCKET_NAME).build());
            System.out.println("[INFO][LOCAL APP] Bucket created successfully.");
        }
    }

    /**
     * Checks if a Manager EC2 instance is currently running.
     * Queries EC2 for instances with tag "Role=Manager" and state "running".
     * 
     * @return true if at least one Manager instance is running, false otherwise
     */
    private boolean is_manager_running() {
        System.out.println("[INFO][LOCAL APP] Checking EC2 instances with tag Role=Manager...");
        DescribeInstancesResponse r = _ec2.describeInstances(
                DescribeInstancesRequest.builder()
                        .filters(
                                Filter.builder().name("tag:Role").values("Manager").build(),
                                Filter.builder().name("instance-state-name").values("running").build())
                        .build());

        boolean running = !r.reservations().isEmpty();
        System.out.println("[INFO][LOCAL APP] Manager running: " + running);
        return running;
    }

    /**
     * Launches a new Manager EC2 instance.
     *
     * Properties:
     * - Uses the pre-built AMI containing:
     *  Java 17 , manager.jar, worker.jar
     * - Tags the instance with Role=Manager
     * - User-data simply runs the Manager process:
     * java -cp manager.jar com.dsp.Manager
     *
     * The AMI already contains all required software,
     * so no installation occurs in user-data.
     *
     * @throws Ec2Exception if the instance launch fails
     */
    private void start_manager() {
        System.out.println("[INFO][LOCAL APP] Starting new Manager EC2 instance...");

        String user_data = Base64.getEncoder().encodeToString(
                ("#!/bin/bash\ncd /opt/dsp-app\njava -cp manager.jar com.dsp.Manager " + _AMI_ID + "\n").getBytes());

        RunInstancesRequest req = RunInstancesRequest.builder()
                .imageId(_AMI_ID)
                .instanceType(InstanceType.T3_MICRO)
                .minCount(1)
                .maxCount(1)
                .userData(user_data)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .name("LabInstanceProfile")
                        .build())
                .tagSpecifications(TagSpecification.builder()
                        .resourceType(ResourceType.INSTANCE)
                        .tags(Tag.builder().key("Role").value("Manager").build())
                        .build())
                .build();

        RunInstancesResponse resp = _ec2.runInstances(req);

        if (resp.instances().isEmpty()) {
            throw new RuntimeException("[ERROR] Failed to start Manager instance.");
        }

        System.out.println("[INFO][LOCAL APP] Manager EC2 instance launched successfully.");
    }

    /**
     * Uploads a local file to the S3 bucket.
     * The object key is the file's base name.
     *
     * @param path local filesystem path
     * @return S3 object key under which the file was stored
     */
    private String upload_file(String path) {
        Path filePath = Paths.get(path);
        String fileName = filePath.getFileName().toString();

        System.out.println("[INFO][LOCAL APP] Uploading file to S3: " + fileName);

        _s3.putObject(
                PutObjectRequest.builder()
                        .bucket(_BUCKET_NAME)
                        .key(fileName)
                        .build(),
                RequestBody.fromFile(filePath));

        System.out.println("[INFO][LOCAL APP] Upload completed: " + fileName);

        return fileName;
    }

    /**
     * Creates an SQS queue with the given name.
     * If the queue already exists, AWS returns its URL.
     *
     * @param name queue name
     * @return queue URL
     */
    private String create_queue(String name) {
        System.out.println("[INFO][LOCAL APP] Creating SQS queue: " + name);

        CreateQueueResponse resp = _sqs.createQueue(
                CreateQueueRequest.builder().queueName(name).build());

        System.out.println("[INFO][LOCAL APP] Queue created: " + resp.queueUrl());
        return resp.queueUrl();
    }

    /**
     * Sends a message to the specified SQS queue.
     *
     * @param queue_url URL of the queue
     * @param msg       message body
     */
    private void send_message(String queue_url, String msg) {
        System.out.println("[INFO][LOCAL APP] Sending message to queue: " + queue_url);

        _sqs.sendMessage(
                SendMessageRequest.builder()
                        .queueUrl(queue_url)
                        .messageBody(msg)
                        .build());

        System.out.println("[INFO][LOCAL APP] Message sent.");
    }

    /**
     * Polls the response queue until a DONE message is received from the Manager.
     * Uses long polling (20 seconds) to reduce API calls and improve efficiency.
     * 
     * Expected message format: "DONE\t<s3_summary_key>"
     * 
     * @param queue_url URL of the response queue to poll
     * @return The S3 key where the result summary file is stored
     * @throws RuntimeException if the message format is invalid or doesn't start with "DONE"
     */
    private String wait_for_completion(String queue_url) {
        System.out.println("[INFO][LOCAL APP] Waiting for DONE message...");

        int pollCount = 0;
        while (true) {
            // Every 5 polls (~2 minutes), check if Manager is still running
            if (pollCount % 6 == 0) {
                if (!is_manager_running()) {
                    throw new RuntimeException(
                            "[ERROR][LOCAL APP] Manager instance has crashed or been terminated!\n" +
                            "Please manually clean up:\n" +
                            "  1. Terminate any running Worker instances\n" +
                            "  2. Delete SQS queues (manager-input-queue, worker-tasks-queue, worker-results-queue)\n" +
                            "  3. Check S3 bucket for orphaned files");
                }
            }
            pollCount++;

            ReceiveMessageResponse req = _sqs.receiveMessage(
                    ReceiveMessageRequest.builder()
                            .queueUrl(queue_url)
                            .waitTimeSeconds(20)
                            .maxNumberOfMessages(1)
                            .visibilityTimeout(60)
                            .build());

            if (req.messages().isEmpty()) {
                System.out.println("[INFO][LOCAL APP] No message yet... polling again...");
                continue;
            }

            Message msg = req.messages().get(0);
            String body = msg.body();

            System.out.println("[INFO][LOCAL APP] Received message: " + body);

            String[] parts = body.split("\t", 2);
            if (parts.length < 2) {
                System.out.println("[WARN][LOCAL APP] Malformed message, ignoring.");
                continue;
            }

            if (parts[0].equals("DONE")) {
                String key = parts[1];
                System.out.println("[INFO][LOCAL APP] Task completed. Summary key: " + key);
                delete_message(queue_url, msg.receiptHandle());
                return key;
            }
        }
    }

    /**
     * Deletes a message from the specified SQS queue.
     * 
     * @param queue URL of the SQS queue
     * @param handle Receipt handle of the message to delete (obtained when receiving the message)
     */
    private void delete_message(String queue, String handle) {
        System.out.println("[INFO][LOCAL APP] Deleting message from queue...");
        _sqs.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(queue)
                .receiptHandle(handle)
                .build());
        System.out.println("[INFO][LOCAL APP] Message deleted.");
    }

    /**
     * Downloads an object from S3 to the local filesystem.
     * 
     * If the file already exists locally, it will be overwritten.
     * 
     * @param key         S3 key (object name) of the file to download
     * @param output_path Local file system path where the file should be saved
     */
    private void download_file(String key, String output_path) {
        System.out.println("[INFO][LOCAL APP] Downloading S3 object: " + key + " to " + output_path);

        Path out = Paths.get(output_path);

        try {
            if (java.nio.file.Files.exists(out)) {
                System.out.println("[INFO][LOCAL APP] Output file already exists. Deleting old file...");
                java.nio.file.Files.delete(out);
            }
        } catch (Exception e) {
            throw new RuntimeException("[ERROR][LOCAL APP] Failed to delete existing output file: " + e.getMessage(),
                    e);
        }

        _s3.getObject(
                GetObjectRequest.builder()
                        .bucket(_BUCKET_NAME)
                        .key(key)
                        .build(),
                out);

        System.out.println("[INFO][LOCAL APP] Download complete.");
    }
}
