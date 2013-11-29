package com.levelup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * User: Yuriy Perepelytsya
 * Date: 11/29/13 : 3:37 PM
 * Powered by IDEA
 */
public class GroupByHourRtbTask{

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException, URISyntaxException {

        GenericRtbMapper mapper = new GenericRtbMapper(){

            @Override
            protected void map(Mapper.Context context) throws Exception {
                urlAuthority();
                //TODO OR from "createdAt" ?- not finded so far
                // Use hourId as outKey = calc from "when" in milliseconds
                Long when = when();
                int hourId = (int) (when == null ? 0l : when / 3600000);
                context.write(new IntWritable(hourId), new Text(asCSV()));
            }
        };

        GenericMultiOutputRtbReducer reducer = new GenericMultiOutputRtbReducer(){

            @Override
            protected void reduce(Writable key, Iterable<Writable> values, Context context) throws IOException, InterruptedException {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd.H'h'");
                String fileName = df.format(new Date(((IntWritable)key).get() * 3600000l));
                for (Writable value : values) {
                    mos.write(value, "", fileName);
                }
            }
        };

        //HadoopTask<IntWritable, Text> task = new HadoopTask<IntWritable, Text>(mapper,reducer,args){};
        new HadoopTask<IntWritable, Text>(mapper,reducer,args);
        //task.startTask(args);
    }
}
