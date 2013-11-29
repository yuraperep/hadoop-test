package com.levelup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * User: Yuriy Perepelytsya
 * Date: 11/29/13 : 11:49 AM
 * Powered by IDEA
 */
public class GroupByHourRtbMapper extends GenericRtbMapper {
    //
//    public static void main(String[] args) {
//        GroupByHourRtbMapper r = new GroupByHourRtbMapper();
//        GenericRtbMapper g = new GenericRtbMapper();
//        System.out.println(r instanceof GenericRtbMapper);
//        System.out.println(g instanceof Mapper);
//        System.out.println(r instanceof Mapper);
//
//    }
//
    @Override
    protected void map(Context context) throws Exception {

        urlAuthority();
        //TODO OR from "createdAt" ?- not finded so far
        // Use hourId as outKey = calc from "when" in milliseconds
        Long when = when();
        int hourId = (int) (when == null ? 0l : when / 3600000);
        context.write(new IntWritable(hourId), new Text(asCSV()));
    }
}
