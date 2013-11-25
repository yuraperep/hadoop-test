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
        return getFieldValueMap(line,"");
    }

    public static Map<String, String> getFieldValueMap(String line,String keyPrefix) {
        String[] pairs = line.split(",");
        Map<String, String> values = new HashMap<>();
        for (String pair : pairs) {
            String[] keyValue = pair.split(":");
            if (keyValue.length == 2) {
                String key = getValue(keyValue[0]);
                if (key != null) {
                    values.put(keyPrefix+key, getValue(keyValue[1]));
                }
            }
        }
        return values;
    }

//{ "uid" : "5768b8fe-cccb-4eeb-b52d-f7015d2aacb7" , "_id" : { "$oid" : "52617b7b26f4b3c367073fa9","time":"1382127311000"} , "__v" : 0}
    public static Map<String, String> getKVwithInnerObjectsMap(String line) {

        Map<String, String> values = new HashMap<>();

        while(true){
            processInnerObjects(values,line);
        }

        values.putAll(getFieldValueMap(line,""));
        return values;
    }

    private static String processInnerObjects(Map<String, String> values, String line) {
        int startOfSearch = line.indexOf("\"");
        int startBrace = line.indexOf("{",startOfSearch);
        int endBrace = line.indexOf("}",startBrace);
        String beforePart = line.substring(0,startBrace);
        int innerKeyStart = beforePart.lastIndexOf("\"[a-z0-9$_\\s]\"");
        if(startBrace>-1 && endBrace>-1 && innerKeyStart>-1){
            String innerObjectKey = beforePart.substring(innerKeyStart,beforePart.indexOf("\"",innerKeyStart));
            String innerObject = line.substring(startBrace+1,endBrace);
            values.putAll(getFieldValueMap(innerObject,innerObjectKey));
            line = line.substring(0,innerKeyStart)+line.substring(endBrace);
        }
        break;
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
