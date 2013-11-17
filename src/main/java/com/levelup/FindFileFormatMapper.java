package com.levelup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static com.levelup.LineParser.getFieldNamesString;

/**
 * Created with IntelliJ IDEA.
 * User: perep80
 * Date: 11/16/13
 * Time: 4:05 PM
 */
public class FindFileFormatMapper extends Mapper<Object, Text, Text, IntWritable> {

    public void map(Object key, Text value, Context context)
            throws InterruptedException, IOException {
        try {
            String line = getFieldNamesString(value.toString());
            for (String field : line.split(",")) {
                context.write(new Text(field), new IntWritable(1));
            }
        } catch (StringIndexOutOfBoundsException e) {
            e.printStackTrace();
        }
    }
}
