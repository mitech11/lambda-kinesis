package com.practice.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.model.S3Event;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

public class Temp implements RequestHandler<S3Object, String> {

    public String handleRequest(S3Object s3Object, Context context) {
        LambdaLogger logger = context.getLogger();



        String bucketName = s3Object.getBucketName();
        String fileName = s3Object.getKey();
        logger.log("Read from bucket: " + bucketName + " and file: " + fileName);
        S3ObjectInputStream inputStream = s3Object.getObjectContent();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            StringBuilder builder = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                logger.log("Records read : " + line);
                builder.append(line);
            }
            return builder.toString();
        } catch (IOException e) {
            logger.log(Arrays.deepToString(e.getStackTrace()));
        }
        return null;
    }
}
