package com.levelup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: perep80
 * Date: 11/16/13
 * Time: 4:06 PM
 */
public class FindFileFormatReducer extends
        Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Set<String> resultFields = new HashSet<>();
        for (Text value : values) {
            String[] fields = value.toString().split(",");
            resultFields.addAll(Arrays.asList(fields));
        }
        context.write(new IntWritable(1), new Text(resultFields.toString()));
    }
}
