package com.levelup.metric;

import com.levelup.CompositeWritable;
import com.levelup.GenericHadoopTask;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created with IntelliJ IDEA.<br/>
 * User: user                 <br/>
 * Date: 11/28/13 - 12:41 PM  <br/>
 * <p/>
 * <br/>Brand Interest Metric<br/>
 * <p/>
 * For each week,user,host we calculate number of day-session combinations<br/>
 * If in some days there was more then N sessions - limit this number to N<br/>
 * if some session lasts several days, calc this as D = number of days<br/>
 */
public class BrandInterestTask<M, R, K, V>
        extends GenericHadoopTask<BrandInterestMapper, BrandInterestReducer, Text, NullWritable> {

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException, URISyntaxException {
        BrandInterestTask<BrandInterestMapper, BrandInterestReducer, Text, NullWritable> task = new BrandInterestTask<>();
        task.startTask(args);
    }

    @Override
    public void configureJob(Job job) {
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CompositeWritable.class);
    }
}
