package org.example.constants;

import com.amazonaws.regions.Regions;

import java.net.URI;

public final class AMAZON_CONSTANTS {

    public static final Regions CLIENT_REGION = com.amazonaws.regions.Regions.AP_SOUTHEAST_2;
    public static final String SECRET_KEY = "9YClKg2GVCgIGP82dNwOJldbq3hcgC0qRhbtqZal";
    public static final String ACCESS_KEY = "AKIAZUO64Q7BB5CLW7MT";
    public static final String BUCKET_NAME = "candidate-108-s3-bucket";
    public static final String ELLIPSIS = "ellipsis";
    public static final String PARQUET_SCHEMA_FILE_EXTENSION = ".avsc";
    public static final String PARQUET_FILE_EXTENSION = ".parquet";
    public static final String PARQUET_FILE_PATH = "target/parquet/";
    public static final String SOURCE_FILE_PATH = "target/source/";
    public static final String CSV_PATH = "target/csv/";
}
