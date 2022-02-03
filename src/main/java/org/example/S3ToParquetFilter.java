package org.example;

import com.amazonaws.services.s3.AmazonS3;
import org.example.authenticate.AmazonS3AuthenticationHandler;
import org.example.io.AmazonParquetFileUpload;
import org.example.io.AmazonS3ZipFileDownload;
import org.example.io.CSVToParquetFileConverter;
import org.example.io.ExtractZip;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class S3ToParquetFilter {

    public static void main(String[] args) throws InterruptedException {

        AmazonS3AuthenticationHandler handler = new AmazonS3AuthenticationHandler();
        AmazonS3 amazonS3 = handler.authenticate();
        ScheduledExecutorService extractService = Executors.newScheduledThreadPool(10);
        if(amazonS3 != null){
            ScheduledFuture extractSchedule = extractService.scheduleAtFixedRate(new Thread(new ExtractZip()), 10, 60, TimeUnit.SECONDS);

            ScheduledFuture zipDownload = extractService.scheduleAtFixedRate(new Thread(new AmazonS3ZipFileDownload(amazonS3)), 30, 120, TimeUnit.SECONDS);

            ScheduledFuture csvToParquet = extractService.scheduleAtFixedRate(new Thread(new CSVToParquetFileConverter(amazonS3,new AmazonParquetFileUpload(amazonS3))), 10, 60, TimeUnit.SECONDS);

            int count = 0;
            while(true){
                System.out.println("count :" + count);
                count++;
                Thread.sleep(100000);
                if(count == 5){
                    System.out.println("Count is 5, cancel the scheduledFuture!");
                    extractSchedule.cancel(true);
                    zipDownload.cancel(true);
                    csvToParquet.cancel(true);
                    extractService.shutdown();
                    break;
                }
            }
        }

    }
}
