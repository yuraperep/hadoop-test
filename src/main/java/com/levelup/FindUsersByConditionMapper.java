package com.levelup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.util.Map;

import static com.levelup.LineParser.getFieldValueMap;

/**
 * Created with IntelliJ IDEA.
 * User: perep80
 * Date: 11/16/13
 * Time: 2:12 PM
 */
public class FindUsersByConditionMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static ScriptEngineManager factory = new ScriptEngineManager();
    private static ScriptEngine engine = factory.getEngineByName("rhino");

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        try {
            String condition = context.getConfiguration().get("condition");
            Map<String, String> kv = getFieldValueMap(value.toString());
            engine.getBindings(ScriptContext.ENGINE_SCOPE).clear();
            engine.getBindings(ScriptContext.ENGINE_SCOPE).putAll(kv);
            if ((boolean) engine.eval(condition)) {
                context.write(new Text(kv.get("user")), new IntWritable(1));
            }
            ;
        } catch (StringIndexOutOfBoundsException | NumberFormatException e) {
            System.out.println("ERROR_IN_LINE=" + value.toString());
            e.printStackTrace();
        } catch (ScriptException e) {
            // for numerous cases like "column with name data16 absent in line"
            //System.out.println(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
