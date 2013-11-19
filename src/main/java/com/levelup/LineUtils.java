package com.levelup;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: perep80
 * Date: 11/16/13
 * Time: 2:32 PM
 */
public class LineUtils {

    public static String convertToCsv(Map<String, String> kv) {
        StringBuilder sb = new StringBuilder();
        String prefix = "";
        for (Map.Entry e : kv.entrySet()) {
            sb.append(prefix).append(e.getKey()).append(":").append(e.getValue());
            prefix = ",";
        }
        return sb.toString();
    }

    public static Map<String, String> getFieldValueMap(String line) {
        String[] pairs = line.split(",");
        Map<String, String> values = new HashMap<>();
        for (String pair : pairs) {
            String[] keyValue = pair.split(":");
            if (keyValue.length == 2) {
                String key = getValue(keyValue[0]);
                if (key != null) {
                    values.put(key, getValue(keyValue[1]));
                }
            }
        }
        return values;
    }

    private static String getValue(String s) {
        int startIndex = s.indexOf("\"");
        if (startIndex > -1) {
            String res = s.substring(startIndex + 1, s.lastIndexOf("\""));
            return res;
        }
        return null;
    }
}
