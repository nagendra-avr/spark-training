package com.training.spark.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;

/**
 * Created by nagi on 5/21/16.
 */
public class SequenceFileWriter {

    private static final String[] DATA = { "One, two, buckle my shoe",
            "Three, four, shut the door", "Five, six, pick up sticks",
            "Seven, eight, lay them straight", "Nine, ten, a big fat hen" };

    public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            System.err.println("Usage: SequenceFileWriter <directoryName>");
            System.exit(1);
        }

        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);
        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(fs, conf, path, key.getClass(),
                    value.getClass());
            for (int i = 0; i < 100; i++) {
                key.set(100 - i);
                value.set(DATA[i % DATA.length]);
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key,
                        value);
                writer.append(key, value);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(writer);
        }
    }
}
