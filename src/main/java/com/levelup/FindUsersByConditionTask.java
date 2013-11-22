package com.levelup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created with IntelliJ IDEA.
 * User: perep80
 * Date: 11/16/13
 * Time: 2:12 PM
 */
public class FindUsersByConditionTask<M, R, K, V>
        extends GenericHadoopTask<FindUsersByConditionMapper, FindUsersByConditionReducer, Text, IntWritable> {

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException, URISyntaxException {
        FindUsersByConditionTask<FindUsersByConditionMapper, FindUsersByConditionReducer, Text, IntWritable> task = new FindUsersByConditionTask<>();
        task.startTask(args);
    }
}
