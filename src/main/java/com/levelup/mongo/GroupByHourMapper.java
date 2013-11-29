package com.levelup.mongo;


import com.levelup.LineUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

import static com.levelup.LineUtils.convertToCsv;

/**
 * Created with IntelliJ IDEA.
 * User: perep80
 * Date: 11/15/13
 * Time: 8:26 PM
 */
public class GroupByHourMapper extends Mapper<Object, Text, IntWritable, Text> {


    /**
     * @param value - JSON object that hold 1 line of text
     *              like {"field1":"value1","field2":"value2"}
     *              <p/>
     *              We should clear value from {} characters,
     *              only right sides of "field1":"value1" should left
     *              Use hourId as outKey = calc from "when" in milliseconds
     */
    public void map(Object key, Text value, Context context)
            throws InterruptedException, IOException {
        try {
            Map<String, String> kv = LineUtils.getKVwithInnerObjectsMap(value.toString());
            // get timestamp from _id
            //int value = Integer.parseInt(hex, 16); - first 4 bytes
            ///OR from createdAt
            // get domain from url


            Long when = Long.valueOf(kv.get("when"));
            int hourId = (int) (when == null ? 0l : when / 3600000);
            String line = convertToCsv(kv);
            context.write(new IntWritable(hourId), new Text(line));
        } catch (StringIndexOutOfBoundsException | NumberFormatException e) {
            context.write(new IntWritable(0), value);
        }
    }
}