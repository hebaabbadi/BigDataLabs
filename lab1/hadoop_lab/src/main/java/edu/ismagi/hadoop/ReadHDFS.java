package edu.ismagi.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
//import java.nio.file.FileSystem;
//import java.nio.file.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Usage: ReadHDFS <HDFS file path>");
            System.exit(1);
        }

        String filePath = args[0];
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(conf);

        Path path = new Path(filePath);

        if (!fs.exists(path)) {
            System.out.println("File " + filePath + " does not exist in HDFS!");
            fs.close();
            System.exit(1);
        }

        FSDataInputStream inputStream = fs.open(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;

        System.out.println("---- File content ----");
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        System.out.println("----------------------");

        reader.close();
        fs.close();
    }
}