package com.levelup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import static com.levelup.LineParser.*;

/**
 * Created with IntelliJ IDEA.
 * User: perep80
 * Date: 11/16/13
 * Time: 2:12 PM
 */
public class FindUsersByConditionMapper extends Mapper<Object, Text, Text, IntWritable> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        try{
            String line = value.toString();
            Integer data4 = getInt(line,"data4");
            if(data4!=null && data4>5000 && data4<8000){
                context.write(new Text(getString(line,"user")),new IntWritable(1));
            }
        }catch(StringIndexOutOfBoundsException | NumberFormatException e){
            e.printStackTrace();
        }
    }
}
