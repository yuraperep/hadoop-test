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

/**
 * Created with IntelliJ IDEA.
 * User: perep80
 * Date: 11/15/13
 * Time: 8:25 PM
 */
public class GroupByHourTask<M, R, K, V>
        extends GenericHadoopTask<GroupByHourMapper, GroupByHourReducer,IntWritable, Text> {

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException, URISyntaxException {
        GroupByHourTask<GroupByHourMapper, GroupByHourReducer,IntWritable, Text> task = new GroupByHourTask<>();
        task.startTask(args);
    }
}
