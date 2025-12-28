package com.dsp;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

/**
 * EC2Node is an abstract base class for distributed system components
 * (Manager and Worker) running on EC2 instances.
 * 
 * Provides common functionality:
 * - AWS client initialization (S3, SQS)
 * - SQS queue operations (create, send, receive, delete)
 * - Shared configuration (region, bucket name)
 * 
 * Subclasses must implement the run() method to define their main execution logic.
 */
public abstract class EC2Node {

    protected final Region region = Region.US_EAST_1;
    protected final S3Client s3;
    protected final SqsClient sqs;

    protected final String bucketName = "dsp-task1-text-analyzer-ben-ori";

    /**
     * Constructs a new EC2Node.
     * Initializes AWS clients for S3 and SQS in the US-EAST-1 region.
     */
    public EC2Node() {
        this.s3 = S3Client.builder().region(region).build();
        this.sqs = SqsClient.builder().region(region).build();
    }

    /**
     * Creates an SQS queue with the specified name.
     * If the queue already exists, returns the URL of the existing queue.
     * 
     * @param name Name of the queue to create
     * @return URL of the created (or existing) queue
     */
    protected String createQueue(String name) {
        CreateQueueRequest req = CreateQueueRequest.builder()
                .queueName(name)
                .build();

        return sqs.createQueue(req).queueUrl();
    }

    /**
     * Sends a message to the specified SQS queue.
     * 
     * @param queueUrl URL of the target SQS queue
     * @param body Message content to send
     */
    protected void sendMessage(String queueUrl, String body) {
        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(body)
                .build());
    }

    /**
     * Receives one message from the specified SQS queue using long polling.
     * 
     * Configuration:
     * - maxNumberOfMessages: 1 (receives at most one message)
     * - waitTimeSeconds: 20 (long polling to reduce API calls)
     * - visibilityTimeout: 120 (message hidden from other consumers for 120 seconds / 2 minutes)
     * 
     * @param queueUrl URL of the SQS queue to poll
     * @return ReceiveMessageResponse (check .messages().isEmpty() to see if a message was received)
     */
    protected ReceiveMessageResponse receiveOne(String queueUrl) {
        return sqs.receiveMessage(
                ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(20)
                        .visibilityTimeout(4000) // 4000 seconds = ~66 minutes
                        .build());
    }

    /**
     * Deletes a message from the specified SQS queue.
     * This should be called after successfully processing a message to prevent reprocessing.
     * 
     * @param queueUrl URL of the SQS queue
     * @param receiptHandle Receipt handle of the message to delete (obtained when receiving the message)
     */
    protected void deleteMessage(String queueUrl, String receiptHandle) {
        sqs.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .build());
    }

    /**
     * Main execution logic for the EC2 node.
     * Must be implemented by subclasses to define their specific behavior.
     */
    public abstract void run();
}
