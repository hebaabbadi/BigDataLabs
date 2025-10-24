package edu.ismagi.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            Path filepath = new Path("/user/root/input/purchases.txt");

            if (!fs.exists(filepath)) {
                System.out.println("File does not exist!");
                System.exit(1);
            }

            FileStatus status = fs.getFileStatus(filepath);
            System.out.println("File Name: " + filepath.getName());
            System.out.println("File Size: " + status.getLen());
            System.out.println("Owner: " + status.getOwner());
            System.out.println("Permission: " + status.getPermission());
            System.out.println("Replication: " + status.getReplication());
            System.out.println("Block Size: " + status.getBlockSize());

            fs.rename(filepath, new Path("/user/root/input/achats.txt"));

            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}