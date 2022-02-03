package org.example.io;

import com.amazonaws.services.s3.AmazonS3;
import org.example.authenticate.AmazonS3AuthenticationHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class AmazonS3AuthenticationHandlerTest {

    AmazonS3AuthenticationHandler amazonS3AuthenticationHandler;

    @BeforeEach
    void setUp() {
        amazonS3AuthenticationHandler = new AmazonS3AuthenticationHandler();
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    @DisplayName("S3 Authentication Test")
    void authenticate() {
        //given
        String region = "ap-southeast-2";
        //when
        AmazonS3 amazonS3 = amazonS3AuthenticationHandler.authenticate();
        //then
        assertNotNull(amazonS3);
        assertEquals(amazonS3.getRegionName(), region);
    }

    /*
    * The authentication parameters like access key
    * can be externalized and injected. In this can it is
    * constant. If it was externalized, the negative test would have
    * been to authenticate with invalid creds
    */
}