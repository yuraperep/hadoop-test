package com.levelup.metric;

import com.levelup.CompositeWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * User: Yuriy Perepelytsya
 * Date: 11/28/13 : 5:12 PM
 * Powered by IDEA
 */
public class BrandInterestReducer extends Reducer<Text, CompositeWritable, Text, NullWritable> {

    private static Integer MAX_SESSIONS_PER_DAY = 10;

    @Override
    public void reduce(Text key, Iterable<CompositeWritable> values, Context context)
            throws IOException, InterruptedException {
        try {
            Map<String, Set<String>> freq = new HashMap<>();

            // group map by first field (Day) with List of second field(Session)
            for (CompositeWritable mapKey : values) {
                Set<String> s = freq.get(mapKey.getFirst());
                if (s == null) {
                    s = new HashSet<>();
                    freq.put(mapKey.getFirst(), s);
                }
                s.add(mapKey.getSecond());
            }

            int count = 0;
            for (Map.Entry<String, Set<String>> entry : freq.entrySet()) {
                count += Math.min(entry.getValue().size(), MAX_SESSIONS_PER_DAY);
            }
            context.write(new Text(key.toString() + ":" + count), null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
