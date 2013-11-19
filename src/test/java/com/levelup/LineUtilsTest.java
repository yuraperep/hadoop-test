package com.levelup;

import org.junit.Test;

import java.util.Map;
import java.util.Properties;

import static com.levelup.LineUtils.*;
import static org.junit.Assert.assertTrue;

/**
 * Created with IntelliJ IDEA.
 * User: perep80
 * Date: 11/18/13
 * Time: 6:51 PM
 */
public class LineUtilsTest {

    Properties prop = new Properties();

    @org.junit.Before
    public void setUp() throws Exception {
        prop.load(getClass().getResourceAsStream("/test.properties"));

    }


    @Test
    public void testGoodString() throws Exception {

        String goodLine = (String) prop.get("goodLine");

        Map<String, String> kv = getFieldValueMap(goodLine);
        assertTrue(kv.get("key1").equals("value1"));
        assertTrue(kv.get("when").equals("12345678"));
        assertTrue(kv.size() == 3);

        String converted = convertToCsv(kv);
        assertTrue(converted.length() == 37);
    }

    @Test
    public void testLineWithSpaces() throws Exception {

        String lineWithSpaces = (String) prop.get("lineWithSpaces");

        Map<String, String> kv = getFieldValueMap(lineWithSpaces);
        assertTrue(kv.get("key1").equals("value1"));
        assertTrue(kv.get("when").equals("12345678"));
        assertTrue(kv.size() == 3);

        String converted = convertToCsv(kv);
        assertTrue(converted.length() == 37);
    }

    @Test
    public void testEmptyLine() throws Exception {

        String emptyLine = (String) prop.get("emptyLine");

        Map<String, String> kv = getFieldValueMap(emptyLine);
        assertTrue(kv.isEmpty());
        String converted = convertToCsv(kv);
        assertTrue(converted.length() == 0);
    }
}
