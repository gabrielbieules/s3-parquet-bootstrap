package org.example.io;

import org.example.constants.AMAZON_CONSTANTS;
import org.example.util.DeleteFileUtility;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class ExtractZip implements Runnable {

    @Override
    public void run() {
        System.out.println("Zip Extract Thread started " + Thread.currentThread().getName());
        try {
            extract();
        } catch(IOException e){
            e.printStackTrace();
        } catch(InterruptedException e){
            e.printStackTrace();
        }
    }

    public void extract() throws IOException, InterruptedException {
        WatchService watchService
                = FileSystems.getDefault().newWatchService();
        Path path = Paths.get(AMAZON_CONSTANTS.SOURCE_FILE_PATH);
        path.register(
                watchService,
                StandardWatchEventKinds.ENTRY_MODIFY);
        WatchKey watchKey;
        while(true){
            watchKey = watchService.poll(6000, TimeUnit.SECONDS);
            if(watchKey != null){
                watchKey.pollEvents().stream().forEach(event ->readAndExtract(event.context().toString()));
            }
            watchKey.reset();
        }
    }

    public void readAndExtract(String fileName){
        System.out.println("Before Extracting file " + fileName + " Thread " + Thread.currentThread().getName());
        if(!validateZip(AMAZON_CONSTANTS.SOURCE_FILE_PATH + fileName)){
            System.out.println("Waiting for Zip to download!!");
            return;
        }
        System.out.println("Started Extracting file " + fileName + " Thread " + Thread.currentThread().getName());
        Path outDir = Paths.get(AMAZON_CONSTANTS.CSV_PATH);
        byte[] buffer = new byte[2048];
        try(FileInputStream fis = new FileInputStream(AMAZON_CONSTANTS.SOURCE_FILE_PATH + fileName);
            BufferedInputStream bis = new BufferedInputStream(fis);
            ZipInputStream stream = new ZipInputStream(bis)) {
            ZipEntry entry;
            while((entry = stream.getNextEntry()) != null){
                Path filePath = outDir.resolve(entry.getName());
                try(FileOutputStream fos = new FileOutputStream(filePath.toFile());
                    BufferedOutputStream bos = new BufferedOutputStream(fos, buffer.length)) {
                    int len;
                    while((len = stream.read(buffer)) > 0){
                        bos.write(buffer, 0, len);
                    }
                }
            }
        }catch(IOException exception){
            exception.printStackTrace();
        }
        finally {
                System.out.println("Zip file name before deleting " + fileName);
                deleteReadFiles(AMAZON_CONSTANTS.SOURCE_FILE_PATH, fileName);
        }
    }

    private void deleteReadFiles(String sourcePath, String fileName) {
        DeleteFileUtility deleteFileUtility = new DeleteFileUtility(sourcePath, fileName);
        deleteFileUtility.deleteFile();
    }

    public boolean validateZip(String fileName) {
        ZipFile zipfile = null;
        try {
            zipfile = new ZipFile(new File(fileName));
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            try {
                if (zipfile != null) {
                    zipfile.close();
                    zipfile = null;
                }
            } catch (IOException e) {
            }
        }
    }
}
