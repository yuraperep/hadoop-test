package com.levelup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

public class FindFileFormatTask {

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException, URISyntaxException {

        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        // Create configuration
        Configuration conf = new Configuration(true);

        // Create job
        Job job = new Job(conf, "FindFileFormatTask");
        job.setJarByClass(FindFileFormatMapper.class);

        // Setup MapReduce
        job.setMapperClass(FindFileFormatMapper.class);
        job.setCombinerClass(FindFileFormatReducer.class);
        job.setReducerClass(FindFileFormatReducer.class);
        job.setNumReduceTasks(1);

        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        // Execute job
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
    }

}
