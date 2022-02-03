package org.example.io;

import com.amazonaws.services.s3.AmazonS3;
import org.example.authenticate.AmazonS3AuthenticationHandler;
import org.example.constants.AMAZON_CONSTANTS;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class CSVToParquetFileConverterTest {

    CSVToParquetFileConverter csvToParquetFileConverter;

    AmazonS3 amazonS3;

    @BeforeEach
    void setUp() {
        AmazonS3AuthenticationHandler authenticationHandler = new AmazonS3AuthenticationHandler();
        amazonS3 = authenticationHandler.authenticate();
        AmazonS3ZipFileDownload zipFileDownload = new AmazonS3ZipFileDownload(amazonS3);
        zipFileDownload.download();
        ExtractZip extractZip = new ExtractZip();
        List<File> files = Stream.of(new File(AMAZON_CONSTANTS.SOURCE_FILE_PATH).listFiles())
                .filter(file -> !file.isDirectory())
                .collect(Collectors.toList());
        files.forEach(file -> extractZip.readAndExtract(file.getName()));
        csvToParquetFileConverter = new CSVToParquetFileConverter(amazonS3,new AmazonParquetFileUpload(amazonS3));
    }

    @AfterEach
    void tearDown() {
    }

    /*
    *This Test does all the task of
    * 1. Connecting to AWS
    * 2. Downloading the ZIP file
    * 3. Extracting CSV
    * 4. PArsing CSV, creating parquet file and uploading back to S3
    * Assertions can be done better, by fetching the files again
    * And validating it.
     */
    @Test
    void convertCSVToParquet() {
        //given
        //when
        List<File> files = Stream.of(new File(AMAZON_CONSTANTS.CSV_PATH).listFiles())
                .filter(file -> !file.isDirectory())
                .collect(Collectors.toList());
        int csvFilesBeforeProcessing = files.size();
        files.forEach(file -> csvToParquetFileConverter.convertCSVToParquet(file.getName()));
        //then
        int parquetFiles = Stream.of(new File(AMAZON_CONSTANTS.PARQUET_FILE_PATH).listFiles())
                .filter(file -> !file.isDirectory())
                .collect(Collectors.toList()).size();
        assertEquals(parquetFiles,0);
    }
}