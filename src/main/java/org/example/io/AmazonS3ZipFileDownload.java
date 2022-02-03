package org.example.io;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.io.FileUtils;
import org.example.constants.AMAZON_CONSTANTS;

import java.io.File;
import java.io.IOException;

public class AmazonS3ZipFileDownload implements Runnable {

    AmazonS3 s3client;

    public AmazonS3ZipFileDownload(AmazonS3 s3client) {
        this.s3client = s3client;
    }

    public void download(){
        ObjectListing objectListing = s3client.listObjects(AMAZON_CONSTANTS.BUCKET_NAME);
        for(S3ObjectSummary os : objectListing.getObjectSummaries()){
            System.out.println("key " + os.getKey());
            S3Object fullObject = s3client.getObject(new GetObjectRequest(AMAZON_CONSTANTS.BUCKET_NAME, os.getKey()));
            if(os.getKey().toString().endsWith(".zip")){
                try {
                    FileUtils.copyInputStreamToFile(fullObject.getObjectContent(), new File(AMAZON_CONSTANTS.SOURCE_FILE_PATH + os.getKey()));
                } catch(IOException e){
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void run() {
        System.out.println("S3 Download Thread started " + Thread.currentThread().getName());
        download();
    }
}
