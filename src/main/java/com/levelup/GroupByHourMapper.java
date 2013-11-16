package com.levelup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import static com.levelup.LineParser.*;

/**
 * Created with IntelliJ IDEA.
 * User: perep80
 * Date: 11/15/13
 * Time: 8:26 PM
  */
public class GroupByHourMapper extends Mapper<Object, Text, IntWritable, Text> {


    /**
     * @param value - JSON object that hold 1 line of text
     *     like {"field1":"value1","field2":"value2"}
     *
     * We should clear value from {} characters,
     * only right sides of "field1":"value1" should left
     * Use hourId as outKey = calc from "when" in milliseconds
     *
     */
    public void map(Object key, Text value, Context context)
            throws InterruptedException, IOException {
        try{
            String line = value.toString();
            Long when = getLong(line,"when");
            if(when==null)when=0l;
            int hourId = (int) (when/3600000);
            line = convertToCsv(line);
            context.write(new IntWritable(hourId),new Text(line));
        }catch(StringIndexOutOfBoundsException | NumberFormatException e){
            context.write(new IntWritable(0),value);
        }
    }
}
