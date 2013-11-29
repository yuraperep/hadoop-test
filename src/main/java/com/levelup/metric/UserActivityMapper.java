package com.levelup.metric;

import com.levelup.GenericRtbMapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: user
 * Date: 11/28/13
 * Time: 1:36 PM
 */
public class UserActivityMapper extends GenericRtbMapper {

    @Override
    public void map(Context context) throws IOException, InterruptedException {
        String outKey = user() + ":" + session() + ":" + day() + ":" + urlAuthority();
        context.write(new Text(outKey), ONE);
    }
}
