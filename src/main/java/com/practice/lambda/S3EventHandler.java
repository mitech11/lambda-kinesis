package com.practice.lambda;

import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;

abstract class S3EventHandler implements RequestHandler<S3Event, String> {

/*    private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

    @Override
    public String handleRequest(S3Event event, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("event: " + event);

        // Get the object from the event and show its content type
        S3EventNotification.S3Entity s3Entity = event.getRecords().get(0).getS3();
        String bucketName = s3Entity.getBucket().getName();
        String fileName = s3Entity.getObject().getKey();
        logger.log("Read from bucket: " + bucketName + " and file: " + fileName);

        try {
            S3Object response = s3.getObject(new GetObjectRequest(bucketName, fileName));
            ObjectMetadata metadata = response.getObjectMetadata();
            S3ObjectInputStream inputStream = response.getObjectContent();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                StringBuilder builder = new StringBuilder();
                builder.append("Sample");
                while ((line = reader.readLine()) != null) {
                    logger.log("Records read : " + line);
                    builder.append(line);
                }
                String data = builder.toString();

                PutObjectRequest putRequest = new PutObjectRequest("practice-lambda-hello-output", String.valueOf(System.currentTimeMillis()),
                        new ByteArrayInputStream(data.getBytes()), metadata);

                s3.putObject(putRequest);
                return data;
            }
        } catch (Exception e) {
            logger.log(Arrays.deepToString(e.getStackTrace()));
        }
        return null;
    }*/
}
