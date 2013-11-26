package com.levelup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

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
