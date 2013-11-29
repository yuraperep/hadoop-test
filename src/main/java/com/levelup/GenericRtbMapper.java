package com.levelup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map;

import static com.levelup.LineUtils.*;

/**
 * User: Yuriy Perepelytsya
 * Date: 11/29/13 : 11:47 AM
 * Powered by IDEA
 */
public class GenericRtbMapper extends Mapper<Object, Text, Writable, Writable> {

    public static IntWritable ONE = new IntWritable(1);

    protected Map<String, String> kv;

    protected String oid() {
        return kv.get(OBJECT_ID);
    }

    protected String user() {
        return kv.get(USER_ID);
    }

    protected String asCSV() {
        return convertToCsv(kv);
    }

    protected String session() {
        String session = kv.get(SESSION_ID);
        return session==null?"":session;
    }

    protected String day() {
        return getFormattedDate(oid(), DAY_FORMAT);
    }

    protected String week() {
        return getWeekAsFormattedDate(oid());
    }



    protected Long when() {
        String when = kv.get(WHEN);
        if (when == null) {
            when = Long.toString(getTimestampAsLongFromHexOid(oid()));
            kv.put(WHEN, when);
        }
        return Long.valueOf(when);
    }

    protected String urlAuthority() throws MalformedURLException {
        String url = kv.get(URL);
        if (url == null) {
            url = getAuthorityFromURL(kv.get(REFERER));
            kv.put(URL, url);
        }
        return url;
    }

    public void map(Object key, Text value, Context context)
            throws InterruptedException, IOException {
        try {

            kv = LineUtils.getKVwithInnerObjectsMap(value.toString());
            map(context);

        } catch (StringIndexOutOfBoundsException | NumberFormatException e) {
            System.out.println("ERROR_LINE=" + value.toString());
            //context.write(new IntWritable(0), value);
        } catch (Exception e) {
            System.out.println("ERROR_LINE=" + value.toString());
        }
    }

    protected void map(Context context) throws Exception {
    }
}
