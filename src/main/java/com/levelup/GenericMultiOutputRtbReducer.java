package com.levelup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * User: Yuriy Perepelytsya
 * Date: 11/29/13 : 2:33 PM
 * Powered by IDEA
 */
public class GenericMultiOutputRtbReducer  extends
        Reducer<Writable, Writable, Writable, Writable> {

    protected MultipleOutputs mos;

    @Override
    public void setup(Context context) {
        mos = new MultipleOutputs(context);
    }

    protected int sum(Iterable<Writable> values){
        int count = 0;
        for (Writable value : values) {
            count += ((IntWritable)value).get();
        }
        return count;
    }

    @Override
    public void cleanup(Context context) throws IOException {
        try {
            mos.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void reduce(Writable key, Iterable<Writable> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, new IntWritable(sum(values)));
    }
}
