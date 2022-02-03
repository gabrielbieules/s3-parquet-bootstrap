package org.example.io;

import com.amazonaws.services.s3.AmazonS3;
import org.example.authenticate.AmazonS3AuthenticationHandler;
import org.example.constants.AMAZON_CONSTANTS;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class AmazonS3ZipFileDownloadTest {

    AmazonS3ZipFileDownload amazonS3ZipFileDownload;
    @BeforeEach
    void setUp() {
        AmazonS3AuthenticationHandler amazonS3AuthenticationHandler = new AmazonS3AuthenticationHandler();
        AmazonS3 amazonS3 = amazonS3AuthenticationHandler.authenticate();
        amazonS3ZipFileDownload = new AmazonS3ZipFileDownload(amazonS3);
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void download() {
        //given

        //when
         amazonS3ZipFileDownload.download();
        //then
        assertNotEquals(Stream.of(new File(AMAZON_CONSTANTS.SOURCE_FILE_PATH).listFiles())
                .filter(file -> !file.isDirectory())
                .collect(Collectors.toList()).size(),0);
    }

    /*Negative test includes
    * download should not be tried with
    * failed authentication
    */

}