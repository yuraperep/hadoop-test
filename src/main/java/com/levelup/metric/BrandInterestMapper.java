package com.levelup.metric;

import com.levelup.CompositeWritable;
import com.levelup.GenericRtbMapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: user
 * Date: 11/28/13
 * Time: 12:41 PM
 */
public class BrandInterestMapper extends GenericRtbMapper {

    @Override
    //  For each week,user,host we calculate number of day-session combinations
    public void map(Context context) throws IOException, InterruptedException {
        String outKey = user() + ":" + urlAuthority() + ":" + week();
        CompositeWritable outValue = new CompositeWritable(day(), session());
        context.write(new Text(outKey), outValue);
    }
}
