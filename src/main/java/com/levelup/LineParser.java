package com.levelup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: perep80
 * Date: 11/16/13
 * Time: 2:32 PM
 */
public class LineParser {

    public static String getString(String line, String fieldName) {
        int fieldStart = line.indexOf("\"", line.indexOf(fieldName) + fieldName.length() + 2);
        if (fieldStart > -1)
            return line.substring(fieldStart + 1, line.indexOf("\"", fieldStart + 1));
        else
            return null;
    }

    public static Integer getInt(String line, String fieldName) {
        try {
            String res = getString(line, fieldName);
            if (res != null)
                return Integer.parseInt(res);
            else
                return null;
        } catch (Exception e) {
            return null;
        }
    }

    public static Long getLong(String line, String fieldName) {
        try {
            String res = getString(line, fieldName);
            if (res != null)
                return Long.parseLong(res);
            else
                return null;
        } catch (Exception e) {
            return null;
        }
    }

    private static String clearLine(String line) {
        int startIndex = line.lastIndexOf("{");
        int endIndex = line.indexOf("}");
        if (startIndex == -1 || endIndex == -1) {
            return "";
        }
        return line.substring(startIndex + 1, endIndex);
    }

    public static String convertToCsv(String line) {
        Map<String, String> kv = getFieldValueMap(line);
        StringBuilder sb = new StringBuilder();
        String prefix = "";
        for (Map.Entry e : kv.entrySet()) {
            sb.append(prefix).append(e.getKey()).append(":").append(e.getValue());
            prefix = ",";
        }
        return sb.toString();
    }

    private static String replaceValues(String line) {
        String clearLine = clearLine(line);
        return clearLine.replaceAll(":\\s*\"[a-z0-9.]+\"\\s*", "");
    }

    private static List<String> splitStringNoSpaces(String line) {
        String[] fields = line.split(",");
        List<String> result = new ArrayList<>();
        for (String field : fields) {
            if (field.indexOf("\"") > -1)
                result.add(field.substring(field.indexOf("\"") + 1, field.lastIndexOf("\"")));
        }
        return result;
    }

    public static List<String> getFieldNames(String line) {
        String fields = replaceValues(line);
        return splitStringNoSpaces(fields);
    }

    public static Map<String, String> getFieldValueMap(String line) {
        String clearLine = clearLine(line).replace("\"", "");
        String[] pairs = clearLine.split(",");
        Map<String, String> values = new HashMap<>();
        for (String pair : pairs) {
            String[] keyValue = pair.split(":");
            if (keyValue.length == 2) {
                values.put(keyValue[0].trim(), keyValue[1].trim());
            }
        }
        return values;
    }

}
