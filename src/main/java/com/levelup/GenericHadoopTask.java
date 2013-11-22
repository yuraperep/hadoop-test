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
 * Created with IntelliJ IDEA.
 * User: perep80
 * Date: 11/19/13
 * Time: 3:22 PM
 */
public abstract class GenericHadoopTask<M extends Mapper, R extends Reducer, OutKey, OutVal > {

    public Class getClassByGenericTypeIndex(int i) {
        ParameterizedType superclass = (ParameterizedType)(getClass().getGenericSuperclass());
        return (Class) superclass.getActualTypeArguments()[i];
    }

    protected void startTask(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        String jobName = args[2];
        String condition = args[3];

        // Create configuration
        Configuration conf = new Configuration(true);
        conf.set("condition", condition);

        // Create job
        Job job = new Job(conf,jobName);
        //job.setJarByClass(mapperType);
        job.setJarByClass(getClassByGenericTypeIndex(0));

        // Setup MapReduce
        job.setMapperClass(getClassByGenericTypeIndex(0));
        job.setReducerClass(getClassByGenericTypeIndex(1));
        job.setNumReduceTasks(1);

        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        // Specify key / value
        job.setOutputKeyClass(getClassByGenericTypeIndex(2));
        job.setOutputValueClass(getClassByGenericTypeIndex(3));

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

    public void configureJob(Job job){};

}
