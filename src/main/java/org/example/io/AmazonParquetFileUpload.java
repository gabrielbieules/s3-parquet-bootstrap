package org.example.io;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.apache.commons.io.FilenameUtils;
import org.example.constants.AMAZON_CONSTANTS;
import org.example.util.DeleteFileUtility;

import java.io.File;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class AmazonParquetFileUpload{

    AmazonS3 s3Client;

    public AmazonParquetFileUpload(AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    public void uploadParquetFileToS3(String fileName) {
        System.out.println("Started Parquet File Upload " + fileName + " Thread " + Thread.currentThread().getName()
                + " Time " + LocalDateTime.now());
        File file = new File(AMAZON_CONSTANTS.PARQUET_FILE_PATH + fileName);
        //validateParquetFile(file);
        long contentLength = file.length();
        long partSize = 5 * 1024 * 1024;
        System.out.println("Started Parquet File Upload " + fileName + " Thread " + Thread.currentThread().getName()
                + " File Size MB " + (double)contentLength/(1024 * 1024));
        try {
            List<PartETag> partETags = new ArrayList<>();

            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(AMAZON_CONSTANTS.BUCKET_NAME, fileName);
            InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);

            long filePosition = 0;
            for(int i = 1; filePosition < contentLength; i++){
                // Because the last part could be less than 5 MB, adjust the part size as needed.
                partSize = Math.min(partSize, (contentLength - filePosition));

                // Create the request to upload a part.
                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(AMAZON_CONSTANTS.BUCKET_NAME)
                        .withKey(fileName)
                        .withUploadId(initResponse.getUploadId())
                        .withPartNumber(i)
                        .withFileOffset(filePosition)
                        .withFile(file)
                        .withPartSize(partSize);

                // Upload the part and add the response's ETag to our list.
                UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
                partETags.add(uploadResult.getPartETag());

                filePosition += partSize;
            }
            // Complete the multipart upload.
            CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(AMAZON_CONSTANTS.BUCKET_NAME, fileName,
                    initResponse.getUploadId(), partETags);
            s3Client.completeMultipartUpload(compRequest);
            System.out.println("Parquet File Uploaded to S3 " + fileName + " Thread " + Thread.currentThread().getName());
            deleteReadFiles(AMAZON_CONSTANTS.PARQUET_FILE_PATH, fileName);
            deleteReadFiles(AMAZON_CONSTANTS.PARQUET_FILE_PATH, "."+ fileName+".crc");
        } catch(AmazonServiceException e){
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        } catch(SdkClientException e){
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }
    }

    private void deleteReadFiles(String csvPath, String fileName) {
        DeleteFileUtility deleteFileUtility = new DeleteFileUtility(csvPath, fileName);
        deleteFileUtility.deleteFile();
        DeleteFileUtility deleteCsvFileUtility = new DeleteFileUtility(AMAZON_CONSTANTS.CSV_PATH, FilenameUtils.removeExtension(fileName)+".csv");
        deleteCsvFileUtility.deleteFile();
    }
}
