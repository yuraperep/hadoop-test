package com.levelup;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: perep80
 * Date: 11/16/13
 * Time: 2:32 PM
 */
//TODO add KV holder with DSL like kv.path etc
// or put this logic to e.g. AbstractMongoExportMapper with DSL inside - better !!!
public class LineUtils {

    public static String USER_ID = "uid";
    public static String SESSION_ID = "sessionUid";
    public static String OBJECT_ID = "_id_$oid";
    public static String IP = "remoteAddress";
    public static String USER_AGENT = "userAgent";
    public static String URL = "url";
    // Resource user was tracked on . Goes from HTTP header.
    //"referer" : "http://tst-js.lvlp.co/"
    public static String REFERER = "referer";
    public static String WHEN = "when";
    public static String DAY_FORMAT = "yyyy-MM-dd";


    private static char DOUBLE_QUOT = '"';

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
        return getFieldValueMap(line, "");
    }

    public static Map<String, String> getFieldValueMap(String line, String keyPrefix) {
        String[] pairs = line.split(",");
        Map<String, String> values = new HashMap<>();
        for (String pair : pairs) {
            String[] keyValue = pair.split(":");
            if (keyValue.length > 1) {
                String key = getKey(keyValue[0]);
                if (key != null) {
                    values.put(keyPrefix + key, getValue(line, pair, keyValue[0]));
                }
            }
        }
        return values;
    }

    private static String getValue(String line, String pair, String key) {
        int startInd = line.indexOf(pair);
        if (startInd > -1) {
            startInd = line.indexOf(key, startInd);
            int valueStartInd = line.indexOf(DOUBLE_QUOT, startInd + key.length());
            int valueEndInd = line.indexOf(DOUBLE_QUOT, valueStartInd + 1);
            if (valueStartInd > -1 && valueEndInd > -1)
                return line.substring(valueStartInd + 1, valueEndInd);
        }
        return null;
    }

    /**
     * Parse inner objects as separated strings,
     * using key value as inner object's column name prefix, e.g.
     * <p/>
     * {{"key1":"value1","key2":{"keyInner":"value2"},"when":"12345678"}
     * will be converted to map
     * key1 = value1
     * key2_keyInner = value2
     * when = 12345678
     */
    public static Map<String, String> getKVwithInnerObjectsMap(String line) {

        Map<String, String> values = new HashMap<>();

        int startOfSearch = line.indexOf(DOUBLE_QUOT);
        while (true) {

            int startBrace = line.indexOf("{", startOfSearch);
            int endBrace = line.indexOf("}", startBrace);
            if (startBrace > -1 && endBrace > -1) {
                String innerObject = line.substring(startBrace + 1, endBrace);
                if (innerObject.indexOf(DOUBLE_QUOT) > -1) {
                    String beforePart = line.substring(startOfSearch, startBrace - 1);
                    String[] beforeParts = beforePart.split(",");
                    String innerObjectKeyPart = beforeParts[beforeParts.length - 1];
                    String innerObjectKey = innerObjectKeyPart.substring(innerObjectKeyPart.indexOf(DOUBLE_QUOT) + 1, innerObjectKeyPart.lastIndexOf(DOUBLE_QUOT));
                    values.putAll(getFieldValueMap(innerObject, innerObjectKey + "_"));
                    line = line.substring(0, line.lastIndexOf(innerObjectKeyPart)) + line.substring(endBrace + 1);
                } else {
                    startOfSearch = endBrace;
                }

            } else {
                break;
            }
            ;
        }

        values.putAll(getFieldValueMap(line, ""));
        return values;
    }

    /**
     * Extract value between first two " symbols
     */
    private static String getKey(String s) {

        int startIndex = s.indexOf(DOUBLE_QUOT);
        if (startIndex > -1 && s.lastIndexOf(DOUBLE_QUOT) > startIndex) {
            return s.substring(startIndex + 1, s.lastIndexOf(DOUBLE_QUOT));
        }
        return null;
    }

    /**
     * MondoDb objectId is a composite structure ,
     * with first 4-byte representing the seconds since the Unix epoch
     * http://docs.mongodb.org/manual/reference/object-id/
     * This function extract this seconds part and return milliseconds
     *
     * @param oidHex
     * @return milliseconds since the Unix epoch
     */
    public static long getTimestampAsLongFromHexOid(String oidHex) {
        long value = Integer.parseInt(oidHex.substring(0, 8), 16) * 1000l;
        return value;
    }

    public static String getFormattedDate(String oidHex, String format) {
        long time = getTimestampAsLongFromHexOid(oidHex);
        SimpleDateFormat df = new SimpleDateFormat(format);
        return df.format(new Date(time));
    }

    public static String getWeekAsFormattedDate(String oidHex) {
        long time = getTimestampAsLongFromHexOid(oidHex);
        long millisInWeek = 3600000l * 24 * 7;
        time = (time / millisInWeek) * millisInWeek;
        SimpleDateFormat df = new SimpleDateFormat(DAY_FORMAT);
        return df.format(new Date(time));
    }

    public static String getPathFromURL(String url) throws MalformedURLException {
        URL aURL = new URL(url);
        return aURL.getAuthority() + aURL.getPath();
    }

    public static String getAuthorityFromURL(String url) throws MalformedURLException {
        URL aURL = new URL(url);
        return aURL.getAuthority();
    }


}
