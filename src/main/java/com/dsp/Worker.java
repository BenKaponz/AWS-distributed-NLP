package com.dsp;

import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.core.sync.RequestBody;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.Sentence;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.GrammaticalStructure;
import edu.stanford.nlp.trees.GrammaticalStructureFactory;
import edu.stanford.nlp.trees.PennTreebankLanguagePack;
import edu.stanford.nlp.trees.TreebankLanguagePack;

import java.io.Reader;
import java.io.StringReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Scanner;

/**
 * Worker is a processing node in the distributed text analysis system.
 * Each Worker:
 * - Loads the Stanford NLP parser model from the classpath (packaged in the JAR)
 * - Continuously polls the worker-tasks-queue for sub-tasks
 * - Downloads text content from URLs
 * - Performs POS (Part-of-Speech), CONST (Constituency), or DEP (Dependency) parsing
 * - Uploads results to S3
 * - Sends completion messages to the worker-results-queue
 * 
 * Workers are dynamically launched and terminated by the Manager.
 */
public class Worker extends EC2Node {

    /************************************************/
    /*       GLOBAL SINGLETON MODELS                */
    /************************************************/
    
    private static LexicalizedParser PARSER;
    private static GrammaticalStructureFactory GSF;

    private static final String MODEL_PATH = "edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz";

    private String tasksQueueUrl;
    private String resultsQueueUrl;

    /**
     * Main entry point for the Worker application.
     * 
     * @param args Command line arguments (not used)
     */
    public static void main(String[] args) {
        new Worker().run();
    }

    /**
     * Constructs a new Worker instance.
     * Initializes AWS clients via the EC2Node superclass.
     */
    public Worker() {
        super();
    }

    /**
     * Initializes the Stanford NLP parser model and grammatical structure factory.
     * This method is synchronized to ensure the models are loaded only once across all threads.
     * 
     * The model is loaded from the resources directory: edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz
     * The GSF (GrammaticalStructureFactory) is used for accurate dependency parsing.
     * 
     * @throws RuntimeException if the model file cannot be loaded
     */
    private static synchronized void initModels() {
        if (PARSER == null) {
            try {
                System.out.println("[INFO][WORKER] Loading Stanford NLP model (this may take time)...");
                PARSER = LexicalizedParser.loadModel(MODEL_PATH);
                TreebankLanguagePack tlp = new PennTreebankLanguagePack();
                GSF = tlp.grammaticalStructureFactory();
                System.out.println("[INFO][WORKER] Model loaded successfully.");

            } catch (Exception e) {
                System.err.println("[ERROR][WORKER] Error loading model: " + e.getMessage());
                e.printStackTrace();
                throw new RuntimeException("Failed to load NLP model from classpath", e);
            }
        }
    }

    /**
     * Main execution loop of the Worker.
     * 
     * Process:
     * 1. Initializes the NLP models from the classpath (packaged in JAR)
     * 2. Gets URLs for worker-tasks-queue and worker-results-queue
     * 3. Continuously polls for tasks and processes them
     * 4. Runs indefinitely until Manager terminates the EC2 instance
     */
    public void run() {
        try {
            initModels();

            tasksQueueUrl = sqs.getQueueUrl(b -> b.queueName("worker-tasks-queue")).queueUrl();
            resultsQueueUrl = sqs.getQueueUrl(b -> b.queueName("worker-results-queue")).queueUrl();

            System.out.println("[INFO][WORKER] Worker started. Tasks queue: " + tasksQueueUrl);
            System.out.println("[INFO][WORKER] Results queue: " + resultsQueueUrl);

            while (true) {
                try {
                    var response = receiveOne(tasksQueueUrl);

                    if (response.messages().isEmpty()) {
                        continue;
                    }

                    var message = response.messages().get(0);

                    try {
                        process_message(message, tasksQueueUrl, resultsQueueUrl);
                    } catch (Exception e) {
                        System.err.println("[ERROR][WORKER] Error processing message: " + e.getMessage());
                        e.printStackTrace();
                    } finally {
                        // Always delete message if we got this far (worker didn't crash)
                        // If worker crashes before this point, message will reappear after visibility timeout
                        deleteMessage(tasksQueueUrl, message.receiptHandle());
                    }

                } catch (Exception e) {
                    System.err.println("[ERROR][WORKER] Error receiving message: " + e.getMessage());
                }
            }

        } catch (Exception e) {
            System.err.println("[ERROR][WORKER] Worker error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            s3.close();
            sqs.close();
            System.out.println("[INFO][WORKER] Worker shutdown complete.");
        }
    }

    /**
     * Processes a single task message from the worker-tasks-queue.
     * 
     * Expected message format: "{taskId}\t{analysisType}\t{fileUrl}"
     * 
     * Steps:
     * 1. Downloads text from the URL
     * 2. Performs the requested analysis (POS, CONST, or DEP)
     * 3. Uploads the result to S3
     * 4. Sends a completion message to the worker-results-queue
     * 
     * If an error occurs, uploads an error message to S3 and sends the error key
     * to the results queue instead.
     * 
     * @param message The SQS message containing the task details
     * @param tasks_queue_url URL of the tasks queue (for deleting the message)
     * @param results_queue_url URL of the results queue (for sending completion messages)
     */
    private void process_message(Message message,
            String tasks_queue_url,
            String results_queue_url) {
        String body = message.body();
        String[] parts = body.split("\t", 3);

        if (parts.length < 3) {
            System.err.println("[ERROR][WORKER] Invalid message format: " + body);
            return;
        }

        String taskId = parts[0];
        String analysisType = parts[1];
        String fileUrl = parts[2];

        System.out.println("[INFO][WORKER] Processing task: " + taskId + ", type: " + analysisType + ", url: " + fileUrl);

        try {
            String text = download_text(fileUrl);
            String result = run_analysis(analysisType, text);
            String outputKey = upload_result(taskId, analysisType, fileUrl, result);

            String resultMsg = taskId + "\t" + analysisType + "\t" + fileUrl + "\t" + outputKey;
            sendMessage(results_queue_url, resultMsg);

            System.out.println("[INFO][WORKER] Task " + taskId + " completed successfully.");

        } catch (Exception e) {
            System.err.println("[ERROR][WORKER] Error processing task " + taskId + ": " + e.getMessage());
            e.printStackTrace();

            try {
                // Upload error to S3 for logging/debugging
                upload_error(taskId, analysisType, fileUrl, e.getMessage());
                
                // Send error message directly (not S3 key) for HTML display
                String errorMsg = "ERROR: " + e.getMessage();
                String resultMsg = taskId + "\t" + analysisType + "\t" + fileUrl + "\t" + errorMsg;
                sendMessage(results_queue_url, resultMsg);
            } catch (Exception ex) {
                System.err.println("[ERROR][WORKER] Failed to send error message: " + ex.getMessage());
            }
        }
    }

    /**
     * Downloads text content from a given URL.
     * 
     * Uses a 10-second timeout for both connection and reading.
     * Downloads the entire file content with no size limit.
     * 
     * @param url_string The URL to download text from
     * @return The complete text content from the URL
     * @throws Exception if the download fails or times out
     */
    private String download_text(String url_string) throws Exception {
        System.out.println("[INFO][WORKER] Downloading text from: " + url_string);

        URL url = new URL(url_string);
        URLConnection connection = url.openConnection();
        connection.setConnectTimeout(10000);
        connection.setReadTimeout(10000);

        StringBuilder content = new StringBuilder();
        try (Scanner scanner = new Scanner(connection.getInputStream(), StandardCharsets.UTF_8)) {
            scanner.useDelimiter("\\A");
            if (scanner.hasNext())
                content.append(scanner.next());
        }

        String text = content.toString();
        System.out.println("[INFO][WORKER] Downloaded " + text.length() + " characters");
        return text;
    }

    /**
     * Runs the requested NLP analysis on the given text.
     * 
     * Supported analysis types:
     * - POS: Part-of-Speech tagging
     * - CONSTITUENCY: Constituency parsing (phrase structure)
     * - DEPENDENCY: Dependency parsing (grammatical relations)
     * 
     * @param type Type of analysis (POS, CONSTITUENCY, or DEPENDENCY)
     * @param text The text to analyze
     * @return Analysis results as a string, or an error message if analysis fails
     */
    private String run_analysis(String type, String text) {
        StringBuilder result = new StringBuilder();

        try {
            System.out.println("[INFO][WORKER] Starting analysis of type: " + type + "...");
            switch (type.toUpperCase()) {
                case "POS":
                    result.append(analyzePOS(text));
                    break;
                case "CONSTITUENCY":
                    result.append(analyzeConstituency(text));
                    break;
                case "DEPENDENCY":
                    result.append(analyzeDependency(text));
                    break;
                default:
                    result.append("ERROR: Unknown analysis type: ").append(type);
            }
            System.out.println("[INFO][WORKER] Analysis of type " + type + " completed.");
        } catch (Exception e) {
            result.append("ERROR: ").append(e.getMessage());
        }

        return result.toString();
    }

    /**
     * Performs full Part-of-Speech (POS) tagging on the entire text.
     *
     * Uses Stanford's DocumentPreprocessor for proper sentence segmentation and
     * tokenization.
     * For each sentence, a full parse tree is generated using the global PARSER,
     * and the tagged yield (word/TAG pairs) is extracted.
     *
     * The entire file is processed with no sentence limit.
     *
     * @param text The full text to analyze
     * @return POS-tagged output for all sentences
     */
    private String analyzePOS(String text) {
        StringBuilder output = new StringBuilder();

        DocumentPreprocessor dp1 = new DocumentPreprocessor(new StringReader(text));
        int numberOfSentences = 0;
        for (List<HasWord> ignored : dp1) {
            numberOfSentences++;
        }
        System.out.println("[INFO][WORKER] Total sentences for POS tagging: " + numberOfSentences);

        DocumentPreprocessor dp2 = new DocumentPreprocessor(new StringReader(text));
        int counter = 0;

        for (List<HasWord> sentence : dp2) {
            try {
                if (counter % 50 == 0) {
                    System.out.println("[INFO][WORKER] POS Sentence: " + counter + " / " + numberOfSentences);
                }
                counter++;

                Tree parse = PARSER.apply(sentence);
                output.append(Sentence.listToString(sentence)).append("\n");
                output.append(parse.taggedYield()).append("\n\n");

            } catch (Exception e) {
                output.append("Error: ").append(e.getMessage()).append("\n");
            }
        }

        return output.toString();
    }

    /**
     * Performs full constituency parsing (phrase-structure parsing) on the entire
     * text.
     *
     * Uses Stanford's DocumentPreprocessor for accurate sentence splitting and
     * tokenization.
     * For each sentence, a complete constituency tree is produced using the global
     * PARSER.
     *
     * The entire file is processed with no sentence limit.
     *
     * @param text The full text to analyze
     * @return Constituency parse trees for all sentences
     */
    private String analyzeConstituency(String text) {
        StringBuilder output = new StringBuilder();

        DocumentPreprocessor dpCount = new DocumentPreprocessor(new StringReader(text));
        int totalSentences = 0;
        for (List<HasWord> ignored : dpCount) {
            totalSentences++;
        }
        System.out.println("[INFO][WORKER] Total CONSTITUENCY sentences: " + totalSentences);

        DocumentPreprocessor dp = new DocumentPreprocessor(new StringReader(text));
        int counter = 0;

        for (List<HasWord> sentence : dp) {
            try {
                if (counter % 50 == 0) {
                    System.out.println("[INFO][WORKER] CONSTITUENCY Sentence: " + counter + " / " + totalSentences);
                }
                counter++;

                Tree parse = PARSER.apply(sentence);
                output.append(Sentence.listToString(sentence)).append("\n");
                output.append(parse.toString()).append("\n\n");

            } catch (Exception e) {
                output.append("Error: ").append(e.getMessage()).append("\n");
            }
        }

        return output.toString();
    }

    /**
     * Performs full dependency parsing (typed dependency extraction) on the entire
     * text.
     *
     * Sentences are obtained and tokenized via Stanford's DocumentPreprocessor.
     * Each sentence is parsed using the global PARSER, and dependencies are
     * produced
     * using the global GrammaticalStructureFactory.
     *
     * The entire file is processed with no sentence limit.
     *
     * @param text The full text to analyze
     * @return Dependency relations for all sentences
     */
    private String analyzeDependency(String text) {
        StringBuilder output = new StringBuilder();

        DocumentPreprocessor dpCount = new DocumentPreprocessor(new StringReader(text));
        int totalSentences = 0;
        for (List<HasWord> ignored : dpCount) {
            totalSentences++;
        }
        System.out.println("[INFO][WORKER] Total DEPENDENCY sentences: " + totalSentences);

        DocumentPreprocessor dp = new DocumentPreprocessor(new StringReader(text));
        int counter = 0;

        for (List<HasWord> sentence : dp) {
            try {
                if (counter % 50 == 0) {
                    System.out.println("[INFO][WORKER] DEPENDENCY Sentence: " + counter + " / " + totalSentences);
                }
                counter++;

                Tree parse = PARSER.apply(sentence);
                GrammaticalStructure gs = GSF.newGrammaticalStructure(parse);

                output.append(Sentence.listToString(sentence)).append("\n");
                output.append(gs.typedDependencies()).append("\n\n");

            } catch (Exception e) {
                output.append("Error: ").append(e.getMessage()).append("\n");
            }
        }

        return output.toString();
    }

    /**
     * Uploads analysis results to S3.
     * 
     * Results are stored at: s3://{bucket}/results/{taskId}_{type}.txt
     * 
     * @param taskId Unique identifier for the task
     * @param type Analysis type (POS/CONSTITUENCY/DEPENDENCY)
     * @param url The input URL (for logging purposes, not used in key generation)
     * @param result The analysis result text
     * @return The S3 key where the result was uploaded, or an error message if upload fails
     */
    private String upload_result(String taskId, String type, String url, String result) {
        String outputKey = "results/" + taskId + "_" + type.toLowerCase() + ".txt";

        try {
            System.out.println("[INFO][WORKER] Uploading result to: " + outputKey + " ...");
            s3.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(outputKey)
                            .build(),
                    RequestBody.fromString(result));

            System.out.println("[INFO][WORKER] Uploaded result to: " + outputKey);

        } catch (Exception e) {
            System.err.println("[ERROR][WORKER] Error uploading result: " + e.getMessage());
            return "ERROR: " + e.getMessage();
        }

        return outputKey;
    }

    /**
     * Uploads an error message to S3 when task processing fails.
     * 
     * Error messages are stored at: s3://{bucket}/errors/{taskId}_{type}_error.txt
     * 
     * @param taskId Unique identifier for the task
     * @param type Analysis type (POS/CONSTITUENCY/DEPENDENCY)
     * @param url The input URL that caused the error
     * @param err The error message
     * @return The S3 key where the error was uploaded
     */
    private String upload_error(String taskId, String type, String url, String err) {
        String errorKey = "errors/" + taskId + "_" + type.toLowerCase() + "_error.txt";

        try {
            String content = "Task ID: " + taskId + "\n"
                    + "Analysis Type: " + type + "\n"
                    + "URL: " + url + "\n"
                    + "Error: " + err;

            System.out.println("[INFO][WORKER] Uploading error report to: " + errorKey + " ...");

            s3.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(errorKey)
                            .build(),
                    RequestBody.fromString(content));

            System.out.println("[INFO][WORKER] Uploaded error report to: " + errorKey);

        } catch (Exception e) {
            System.err.println("[ERROR][WORKER] Error uploading error report: " + e.getMessage());
            return "ERROR: Failed to upload error report";
        }

        return errorKey;
    }
}
