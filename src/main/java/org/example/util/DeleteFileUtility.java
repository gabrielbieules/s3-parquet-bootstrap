package org.example.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public final class DeleteFileUtility {

    private String path;
    private String fileName;

    public DeleteFileUtility(String path, String fileName) {
        this.path = path;
        this.fileName = fileName;
    }

    public void deleteFile() {
        try {
            Files.newDirectoryStream(Paths.get(path)).forEach(file -> {
                System.out.println("Delete file " + file.getFileName() +" Thread name " + Thread.currentThread().getName());
                if(file.getFileName().toString().equalsIgnoreCase(fileName)){
                    try {
                        Files.delete(file);
                        System.out.println("File deleted!!!");
                    } catch(IOException e){
                        e.printStackTrace();
                        throw new UncheckedIOException(e);
                    }
                }
            });
        } catch(IOException e){
            e.printStackTrace();
        }
    }
}
