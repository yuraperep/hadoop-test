package com.levelup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: perep80
 * Date: 11/15/13
 * Time: 8:27 PM
 */
public class GroupByHourReducer  extends
        Reducer<IntWritable, Text, Text, IntWritable> {

    private MultipleOutputs mos;

    @Override
    public void setup(Context context) {
        mos = new MultipleOutputs(context);
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
          SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd.H'h'");
          String fileName = df.format(new Date(key.get()*3600000l));
          for (Text value : values) {
            mos.write(value,"", fileName);
          }
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
