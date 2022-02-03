package org.example.authenticate;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.example.constants.AMAZON_CONSTANTS;

public class AmazonS3AuthenticationHandler {

    public AmazonS3 authenticate() {
        AWSCredentials credentials = new BasicAWSCredentials(
                AMAZON_CONSTANTS.ACCESS_KEY,
                AMAZON_CONSTANTS.SECRET_KEY
        );

        AmazonS3 s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(AMAZON_CONSTANTS.CLIENT_REGION)
                .build();
        return s3client;
    }
}
