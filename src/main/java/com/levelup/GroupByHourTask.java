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
 * Created with IntelliJ IDEA.
 * User: perep80
 * Date: 11/15/13
 * Time: 8:25 PM
 */
public class GroupByHourTask<M, R, K, V>
        extends GenericHadoopTask<GroupByHourRtbMapper, GroupByHourReducer, IntWritable, Text> {




    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException, URISyntaxException {

        GenericRtbMapper mapper = new GenericRtbMapper(){

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

            protected void reduce(Writable key, Iterable<Writable> values) throws IOException, InterruptedException {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd.H'h'");
                String fileName = df.format(new Date(((IntWritable)key).get() * 3600000l));
                for (Writable value : values) {
                    mos.write(value, "", fileName);
                }
            }
        };

        GroupByHourTask<GroupByHourRtbMapper, GroupByHourReducer, IntWritable, Text> task = new GroupByHourTask<>();
        task.startTask(args);
    }
}
