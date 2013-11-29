package com.levelup.metric;

import com.levelup.GenericHadoopTask;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created with IntelliJ IDEA.
 * User: user
 * Date: 11/28/13
 * Time: 2:59 PM
 * <p/>
 * Distribution of users by activity =
 * number of actions per combination user-day-session-host
 * TODO Improve - calculate also session duration ?
 */
public class UserActivityTask<M, R, K, V>
        extends GenericHadoopTask<UserActivityMapper, UserActivityReducer, Text, IntWritable> {

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException, URISyntaxException {
        UserActivityTask<UserActivityMapper, UserActivityReducer, Text, IntWritable> task = new UserActivityTask<>();
        task.startTask(args);
    }
}
