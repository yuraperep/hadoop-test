package com.levelup.metric;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: user
 * Date: 11/28/13
 * Time: 2:51 PM
 */
public class UserActivityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private MultipleOutputs mos;

    @Override
    public void setup(Context context) {
        mos = new MultipleOutputs(context);
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int count = 0;
        for (IntWritable value : values) {
            count++;
        }
        mos.write(new Text(key.toString() + ":" + count), "", getFileName(count));

    }

    private String getFileName(int count) {
        if (count < 5)
            return "group_1_to_4";
        else if (count < 21)
            return "group_5_to_20";
        else if (count < 100)
            return "group_21_to_100";
        else
            return "group_over_100";
    }

    @Override
    public void cleanup(Context context) throws IOException {
        try {
            mos.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
