package com.levelup;

import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * User: Yuriy Perepelytsya
 * Date: 11/29/13 : 1:55 PM
 * Powered by IDEA
 */
public class FindFileFormatRtbMapper extends GenericRtbMapper {

    @Override
    public void map(Context context) throws IOException, InterruptedException {
        for (String field : kv.keySet()) {
            context.write(new Text(field), ONE);
        }
    }
}
