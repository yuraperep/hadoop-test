package com.levelup;

import org.apache.hadoop.io.Text;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * User: Yuriy Perepelytsya
 * Date: 11/29/13 : 1:42 PM
 * Powered by IDEA
 */
public class FindUsersByConditionRtbMapper extends GenericRtbMapper {

    private static ScriptEngineManager factory = new ScriptEngineManager();
    private static ScriptEngine engine = factory.getEngineByName("rhino");

    @Override
    protected void map(Context context) throws Exception {

        try {
            String condition = context.getConfiguration().get("condition");
            engine.getBindings(ScriptContext.ENGINE_SCOPE).clear();
            engine.getBindings(ScriptContext.ENGINE_SCOPE).putAll(kv);
            if ((boolean) engine.eval(condition)) {
                context.write(new Text(user()), ONE);
            }
        } catch (ScriptException e) {
            // for numerous cases like "column with name data16 absent in line"
            //System.out.println(e.getMessage());
        }
    }
}
