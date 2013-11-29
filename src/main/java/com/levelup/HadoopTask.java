package com.levelup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.net.URISyntaxException;

/**
 * User: Yuriy Perepelytsya
 * Date: 11/29/13 : 3:33 PM
 * Powered by IDEA
 */
public class HadoopTask<OutKey, OutVal> {

    protected Mapper mapper;
    protected Reducer reducer;

    public HadoopTask(Mapper mapper, Reducer reducer,String[] args) throws InterruptedException, ClassNotFoundException, URISyntaxException, IOException {
        this.mapper = mapper;
        this.reducer = reducer;
        this.startTask(args);
    }

    public Class getClassByGenericTypeIndex(int i) {
        ParameterizedType superclass = (ParameterizedType) (getClass().getGenericSuperclass());
        return (Class) superclass.getActualTypeArguments()[i];
    }

    public void startTask(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        String jobName = args[2];
        String condition = args[3];

        // Create configuration
        Configuration conf = new Configuration(true);
        conf.set("condition", condition);

        // Create job
        Job job = new Job(conf, jobName);
        //job.setJarByClass(mapperType);
        job.setJarByClass(getClass());

        // Setup MapReduce
        job.setMapperClass(mapper.getClass());
        job.setReducerClass(reducer.getClass());
        job.setNumReduceTasks(1);

        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        // Specify key / value
        job.setOutputKeyClass(getClassByGenericTypeIndex(0));
        job.setOutputValueClass(getClassByGenericTypeIndex(1));

        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);

        configureJob(job);

        // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        // Execute job
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
    }

    public void configureJob(Job job) {
    }
}
