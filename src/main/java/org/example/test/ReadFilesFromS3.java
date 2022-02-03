package org.example.test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.io.FileUtils;
import org.example.authenticate.AmazonS3AuthenticationHandler;
import org.example.constants.AMAZON_CONSTANTS;

import java.io.File;
import java.io.IOException;
/**
 *This file is just to download files from S3
 * to verify if the parquet upload was successful
 */
public class ReadFilesFromS3 {

    public static void main(String[] args) throws IOException {
        ReadFilesFromS3 readFilesFromS3 = new ReadFilesFromS3();
        readFilesFromS3.readFilesFromS3();
    }

    public void readFilesFromS3() throws IOException {

        AmazonS3AuthenticationHandler amazonS3AuthenticationHandler = new AmazonS3AuthenticationHandler();
        AmazonS3 s3client = amazonS3AuthenticationHandler.authenticate();

        ObjectListing objectListing = s3client.listObjects(AMAZON_CONSTANTS.BUCKET_NAME);
        for(S3ObjectSummary os : objectListing.getObjectSummaries()){
            System.out.println("key " + os.getKey());
            S3Object fullObject = s3client.getObject(new GetObjectRequest(AMAZON_CONSTANTS.BUCKET_NAME, os.getKey()));
            FileUtils.copyInputStreamToFile(fullObject.getObjectContent(), new File("target/test/" + os.getKey()));
        }

    }
}
