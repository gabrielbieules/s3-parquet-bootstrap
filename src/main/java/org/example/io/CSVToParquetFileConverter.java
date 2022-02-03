package org.example.io;

import com.amazonaws.services.s3.AmazonS3;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.example.constants.AMAZON_CONSTANTS;
import org.example.util.DeleteFileUtility;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CSVToParquetFileConverter implements Runnable {

    AmazonS3 s3Client;

    AmazonParquetFileUpload amazonParquetFileUpload;

    public CSVToParquetFileConverter(AmazonS3 s3Client, AmazonParquetFileUpload amazonParquetFileUpload) {
        this.s3Client = s3Client;
        this.amazonParquetFileUpload = amazonParquetFileUpload;
    }

    public void readCSV() throws IOException, InterruptedException {
        WatchService watchService
                = FileSystems.getDefault().newWatchService();
        java.nio.file.Path path = Paths.get(AMAZON_CONSTANTS.CSV_PATH);
        path.register(
                watchService,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY);
        WatchKey watchKey;
        while(true){
            watchKey = watchService.poll(6000, TimeUnit.SECONDS);
            if(watchKey != null){
                watchKey.pollEvents().stream().forEach(event -> {
                    System.out.println("event " + event.context());
                    convertCSVToParquet(event.context().toString());
                });
            }
            watchKey.reset();
        }
    }

    private List<String> parseCSV(String fileName){
        String file = AMAZON_CONSTANTS.CSV_PATH + fileName;
        List<String> ellipsis = new ArrayList<>();
        try(BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while((line = br.readLine()) != null){
                if(line.trim().contains(AMAZON_CONSTANTS.ELLIPSIS)){
                    ellipsis.add(line);
                }
            }
        }catch(IOException exception){
                exception.printStackTrace();
                return null;
        }
        return ellipsis;
    }

    public void convertCSVToParquet(String fileName){
        System.out.println("Before reading CSV file " + fileName + " Thread " + Thread.currentThread().getName());
        List<String> ellipsis = parseCSV(fileName);
        if(ellipsis!=null){
            List<GenericData.Record> sampleData = new ArrayList<>();
            String fileNameWithOutExt = FilenameUtils.removeExtension(fileName);
            Schema schema = createScehma(fileNameWithOutExt);
            ellipsis.forEach(s -> sampleData.add(genRecord(s, schema)));
            org.apache.hadoop.fs.Path path =
                    new org.apache.hadoop.fs.Path(AMAZON_CONSTANTS.PARQUET_FILE_PATH + fileNameWithOutExt + AMAZON_CONSTANTS.PARQUET_FILE_EXTENSION);
            writeToParquet(sampleData,
                    path
                    , schema, fileName);
        }
    }

    public Schema createScehma(String fileName) {
        Schema schema;
        String schemaLocation = "/" + fileName + AMAZON_CONSTANTS.PARQUET_SCHEMA_FILE_EXTENSION;
        try(InputStream inStream = CSVToParquetFileConverter.class.getResourceAsStream(schemaLocation)) {
            schema = new Schema.Parser().parse(IOUtils.toString(inStream, "UTF-8"));
        } catch(Exception e){
            System.out.println("Can't read SCHEMA file from {}" + schemaLocation);
            throw new RuntimeException("Can't read SCHEMA file from" + schemaLocation, e);
        }
        return schema;
    }

    public GenericData.Record genRecord(String record, Schema schema) {
        GenericData.Record gRecord = new GenericData.Record(schema);
        AtomicInteger count = new AtomicInteger(1);
        String[] splitted = record.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        Arrays.asList(splitted).stream().forEach(s -> gRecord.put("col" + count.getAndIncrement(), s));
        return gRecord;
    }

    public void writeToParquet(List<GenericData.Record> recordsToWrite, org.apache.hadoop.fs.Path fileToWrite, Schema schema, String fileName)
            {
        try(ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(fileToWrite)
                .withSchema(schema)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            for(GenericData.Record record : recordsToWrite){
                System.out.println("Writing Parquet file " + fileName + " Thread " + Thread.currentThread().getName());
                writer.write(record);
            }
        }catch(IOException exception){
            if(exception.getMessage().toString().contains("File already exists")){
                System.out.println("Before deleting file " + fileName + " Thread " + Thread.currentThread().getName());
            }
        }finally {
            amazonParquetFileUpload.uploadParquetFileToS3(FilenameUtils.removeExtension(fileName)+".parquet");
        }
    }

    @Override
    public void run() {
        System.out.println("CSV to Parquet converter Thread started " + Thread.currentThread().getName());
        try {
            readCSV();
        } catch(IOException e){
            e.printStackTrace();
        } catch(InterruptedException e){
            e.printStackTrace();
        }
    }
}
