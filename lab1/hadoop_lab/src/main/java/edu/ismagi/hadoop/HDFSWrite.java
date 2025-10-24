package edu.ismagi.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HDFSWrite {
    public static void main(String[] args) throws IOException {

        if (args.length < 2) {
            System.out.println("Usage: HDFSWrite <HDFS file path> <text to write>");
            System.exit(1);
        }

        String filePath = args[0];
        String message = args[1];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);

        if (fs.exists(path)) {
            System.out.println("File already exists: " + filePath);
        } else {
            FSDataOutputStream outStream = fs.create(path);
            outStream.writeUTF("Bonjour tout le monde !");
            outStream.writeUTF(message);
            outStream.close();
            System.out.println("✅ File created successfully on HDFS: " + filePath);
        }

        fs.close();
    }
}
