Design and implementation guide

Key Points :

1. Consumer and producer model has been implemented for this requirement.
2. Single responsibility principle has been followed.
3. ScheduledExecutorService has been used to implement the logic.
4. Each responsibility(downloading the S3 file, Unzip the S3 file, Upload to S3 etc.) has been handled asynchronously.
5. Each responsibility is a Thread.
6. Sys outs have been used for logging.

Details :

1. S3ToParquetFilter.java is the entry point, this java file will start other threads.
2. AmazonS3AuthenticationHandler.java, is responsible to authenticate with S3 and return AmazonS3, which will be used by other threads to download file, upload file etc.
3. AmazonS3ZipFileDownload.java (Thread-1) is a Thread, which is responsible to download files from S3.
4. ExtractZip.java (Thread-2) is a Thread, which will be polling for a file inside /target/source directory. Once it finds a valid zip file, it extracts the contents to /target/csv/ directory.
5. CSVToParquetFileConverter.java (Thread-3) is a Thread, this will be pooling for a file inside /target/csv directory.
    1. If it finds a csv file, it parses the file and extracts the content with the word "elipsis".
    2. Creates the Parquet file from these lines and pushes back to S3.
6. AmazonParquetFileUpload.java, this file is responsible to upload the Parquet file to S3.
7. ReadFilesFromS3.java, this is a test file just to read all the files from S3 and to verify if Parquet upload was successful.

How it works?

1. Authentication service will authenticate and return the AmazonS3 object.
2. The ScheduledExecutorService, is responsible to schedule the execution of the above-mentioned 3 Threads.
3. Thread-1, will download the Zip file from S3 and places it under /target/source dir.
4. Thread-2, will be pooling for files inside /target/source dir. If it finds a readable zip file, it extracts the contents of zip file inside /target/csv dir and deletes the zip file.
5. Thread-3, will be pooling for files inside /target/csv dir. If it finds the csv file, it parses the file. It extracts the line with the word "elipsis", creates a parquet file out of it. Pushes the parquet file to S3.
6. This process will be repeated based on the configured schedule in ScheduledExecutorService.
7. If a file is missed during an execution, it is guaranteed to be processed during next execution.
8. CSVToParquetFileConverterTest.java, does all the below 4 tasks.
    1. Download zip from S3.
    2. Parse the csv file for "elipsis".
    3. Create parquet file from it.
    4. Push the parquet file back to S3.

** Amazon Access key, Secret key, Bucket name has been removed from AMAZON_CONSTANTS.java