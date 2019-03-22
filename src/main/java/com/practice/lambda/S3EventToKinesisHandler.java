package com.practice.lambda;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.RateLimiter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class S3EventToKinesisHandler implements RequestHandler<S3Event, String> {

    private AmazonKinesis kinesisClient;
    private AmazonS3 s3Client;

    private Properties properties;

    private BlockingQueue<PutRecordsRequestEntry> queue;

    private ExecutorService service;

    private boolean failure;

    private boolean done = false;

    private LambdaLogger logger;

    private ObjectMapper mapper = new ObjectMapper();

    private int shardSize;
    final private RateLimiter rateLimiter = RateLimiter.create(1.0);


    TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
    };

    private void init() throws IOException {
        done = false;
        failure = false;
        properties = new Properties();
        properties.load(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("application.properties")));
        assert !properties.isEmpty() : "Missing properties";
        queue = new ArrayBlockingQueue<>(Integer.parseInt(properties.getProperty("app.batch.queue.capacity", "200")));
        service = Executors.newFixedThreadPool(5);
        shardSize = Integer.parseInt(properties.getProperty("app.batch.shard.size", "2"));

        kinesisClient = AmazonKinesisClientBuilder.defaultClient();
        assert kinesisClient != null && kinesisClient.describeStream(properties.getProperty("app.stream.name")).getStreamDescription().getStreamStatus().equalsIgnoreCase("active");
        s3Client = AmazonS3ClientBuilder.defaultClient();
        shardSize = Integer.parseInt(properties.getProperty("app.batch.shard.size", "2"));
    }

    @Override
    public String handleRequest(S3Event event, Context context) {
        try {
            init();
            String retryBucketName = properties.getProperty("app.s3.retry.bucket.name");
            assert retryBucketName != null : "Retry bucket name is missing";

            String partitionKey = properties.getProperty("app.stream.partition.key");

            assert partitionKey != null : "Partition column missing";

            logger = context.getLogger();

            logger.log("event: " + event);
            S3EventNotification.S3Entity s3Entity = event.getRecords().get(0).getS3();
            String bucketName = s3Entity.getBucket().getName();
            String fileName = s3Entity.getObject().getKey();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (failure) {
                    new CopyObjectRequest(bucketName, fileName, retryBucketName, fileName);
                }
            }));

            logger.log("Read from bucket: " + bucketName + " and file: " + fileName);
            try {
                processS3(bucketName, fileName);
                kinesisWrite();
                service.shutdown();
            /*while (service.isTerminated()) {
                Thread.sleep(1000);
            }*/
            } catch (Exception e) {
                //service.shutdownNow();
            }
        } catch (Exception e) {
            logger.log(Arrays.deepToString(e.getStackTrace()));
        }
        return "";

    }

    private void processS3(String partitionKey, String bucketName, String fileName) {

        service.execute(() -> {
            logger.log("in s3 read - Mitesh");
            S3Object response = s3Client.getObject(new GetObjectRequest(bucketName, fileName));
            S3ObjectInputStream inputStream = response.getObjectContent();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    addRecord(partitionKey, line);
                }
                logger.log("file read done");
                done = true;
            } catch (Exception e) {
                logger.log("Failed to read file: " + e);
            }
        });


    }

    private void processS3(String bucketName, String fileName) {

        service.execute(() -> {
            logger.log("in s3 read - Mitesh");
            S3Object response = s3Client.getObject(new GetObjectRequest(bucketName, fileName));
            S3ObjectInputStream inputStream = response.getObjectContent();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                int partitionNum = 0;
                while ((line = reader.readLine()) != null) {
                    addRecord(partitionNum, line);
                    partitionNum++;
                    partitionNum = partitionNum < shardSize ? partitionNum : 0;
                }
                logger.log("file read done");
                done = true;
            } catch (Exception e) {
                logger.log("Failed to read file: " + e);
            }
        });

    }

    private void addRecord(String partitionKey, String line) throws IOException, InterruptedException {
        HashMap<String, Object> record = mapper.readValue(line, typeRef);
        String partitionValue = record.getOrDefault(partitionKey, new Random().nextInt()).toString();
        PutRecordsRequestEntry putRecordRequest = createPutRecordsRequestEntry(line, partitionValue);
        queue.put(putRecordRequest);
    }

    private void addRecord(int partitionNum, String line) throws InterruptedException {
        String partitionValue = Integer.toString(partitionNum);
        PutRecordsRequestEntry putRecordRequest = createPutRecordsRequestEntry(line, partitionValue);
        queue.put(putRecordRequest);
    }

    private void kinesisWrite() throws InterruptedException {
        String streamName = properties.getProperty("app.stream.name");
        long startTime = System.currentTimeMillis();
        int callPerSecond = 2;

        logger.log("in async job");
        int batchSize = Integer.parseInt(properties.getProperty("app.batch.size", "100"));
        List<PutRecordsRequestEntry> entries = new ArrayList<>();

        double permits = (shardSize * 1000) / batchSize;
        RateLimiter rateLimiter = RateLimiter.create(permits);
        rateLimiter.setRate(permits - 1); //reducing permits for safety
        logger.log("The number of permits is " + permits);
        while (!done || !queue.isEmpty()) {
            PutRecordsRequestEntry requestEntry = queue.take();
            entries.add(requestEntry);
            if (entries.size() >= batchSize) {
                rateLimiter.acquire(1);
                executeRequest(streamName, entries);
                entries.clear();
            }
        }
        logger.log("its not coming here");

        if (!entries.isEmpty()) {
            executeRequest(streamName, entries);
            logger.log("put records job for rest of records");
        }

        logger.log("aync job submitted");
    }

    private PutRecordsResult executeRequest(String streamName, List<PutRecordsRequestEntry> entries) {
        PutRecordsResult putRecordsResult = new PutRecordsResult();
        try {
//            logger.log("Execute put records request");
            PutRecordsRequest request = new PutRecordsRequest();
            request.withRecords(entries);
            request.withStreamName(streamName);
            logger.log("streamName: " + streamName + "and records size: " + entries.size());
            putRecordsResult = kinesisClient.putRecords(request);
            boolean failure = putRecordsResult.getFailedRecordCount() > 0;
            logger.log("Status of put records : " + failure + (putRecordsResult.getFailedRecordCount()));
            if (failure) {
                handleFailedRecords(streamName, putRecordsResult, entries);
            }
        } catch (Exception e) {
            logger.log("Failed to send data: " + e.getStackTrace());
        }
        return putRecordsResult;
    }

    private void handleFailedRecords(String streamName, PutRecordsResult putRecordsResult, List<PutRecordsRequestEntry> putRecordsRequestEntryList) {

        while (putRecordsResult.getFailedRecordCount() > 0) {
            final List<PutRecordsRequestEntry> failedRecordsList = new ArrayList<>();
            final List<PutRecordsResultEntry> putRecordsResultEntryList = putRecordsResult.getRecords();
            for (int i = 0; i < putRecordsResultEntryList.size(); i++) {
                final PutRecordsRequestEntry putRecordRequestEntry = putRecordsRequestEntryList.get(i);
                final PutRecordsResultEntry putRecordsResultEntry = putRecordsResultEntryList.get(i);
                if (putRecordsResultEntry.getErrorCode() != null) {
                    failedRecordsList.add(putRecordRequestEntry);
                }
            }
            putRecordsRequestEntryList = failedRecordsList;
            logger.log("Handling failed Records");
            putRecordsResult = executeRequest(streamName, putRecordsRequestEntryList);
            logger.log("Failed records this time are " + putRecordsResult.getFailedRecordCount());

        }
    }

    private PutRecordsRequestEntry createPutRecordsRequestEntry(String line, String partitionValue) {
        PutRecordsRequestEntry requestEntry = new PutRecordsRequestEntry();
        requestEntry.setPartitionKey("partitionID" + partitionValue);
        requestEntry.withData(ByteBuffer.wrap(line.getBytes()));
        return requestEntry;
    }
}

//https://s3.amazonaws.com/practice-lambda-jar/lambda-hello-1.0-SNAPSHOT.jar