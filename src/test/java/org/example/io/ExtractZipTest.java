package org.example.io;

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

class ExtractZipTest {

    String fileName = "data.zip";

    ExtractZip extractZip;

    @BeforeEach
    void setUp() {
        AmazonS3AuthenticationHandler amazonS3AuthenticationHandler = new AmazonS3AuthenticationHandler();
        AmazonS3ZipFileDownload amazonS3ZipFileDownload = new AmazonS3ZipFileDownload(amazonS3AuthenticationHandler.authenticate());
        amazonS3ZipFileDownload.download();
        extractZip = new ExtractZip();
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void readAndExtract() {
        //given
        //when
        extractZip.readAndExtract(fileName);
        //then
        List<File> files = Stream.of(new File(AMAZON_CONSTANTS.CSV_PATH).listFiles())
                .filter(file -> !file.isDirectory())
                .collect(Collectors.toList());
        assertNotEquals(files.size(),0);
        assertNotEquals(files.stream().filter(file -> file.getName().endsWith(".csv"))
                .collect(Collectors.toList()),0 );
    }
}