package com.levelup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.net.URISyntaxException;

public class FindFileFormatTask<M, R, K, V>
        extends GenericHadoopTask<FindFileFormatRtbMapper, CounterReducer, Text, IntWritable> {

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException, URISyntaxException {
        FindFileFormatTask<FindFileFormatRtbMapper, CounterReducer, Text, IntWritable> task = new FindFileFormatTask<>();
        task.startTask(args);
    }

    @Override
    public void configureJob(Job job) {
        job.setCombinerClass(getClassByGenericTypeIndex(1));
    }
}
