package com.levelup;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.levelup.LineParser.*;
import static org.junit.Assert.*;

/**
 * Created with IntelliJ IDEA.
 * User: perep80
 * Date: 11/18/13
 * Time: 6:51 PM
 */
public class LineParserTest {

    Properties prop = new Properties();

    @org.junit.Before
    public void setUp() throws Exception {
        prop.load(getClass().getResourceAsStream("/test.properties"));

    }


    @org.junit.Test
    public void testGoodString() throws Exception {

        String goodLine = (String) prop.get("goodLine");
        // Field Format
        List<String> fields = LineParser.getFieldNames(goodLine);
        assertTrue(fields.contains("key1"));
        assertTrue(fields.contains("key2"));
        assertTrue(fields.size() == 3);

        // GroupByHour
        Long when = getLong(goodLine, "when");
        assertTrue(12345678l == when);

        String converted = convertToCsv(goodLine);
        assertTrue(converted.length() == 37);

        // FindByCondition
        Map<String, String> kv = getFieldValueMap(goodLine);
        assertEquals(kv.size(), 3);
        assertTrue(kv.get("key1").equals("value1"));
        assertTrue(kv.get("when").equals("12345678"));

    }

    @org.junit.Test
    public void testLineWithSpaces() throws Exception {

        String lineWithSpaces = (String) prop.get("lineWithSpaces");
        // Field Format
        List<String> fields = LineParser.getFieldNames(lineWithSpaces);
        assertTrue(fields.contains("key1"));
        assertTrue(fields.contains("key2"));
        assertTrue(fields.size() == 3);

        // GroupByHour
        Long when = getLong(lineWithSpaces, "when");
        assertTrue(12345678l == when);

        String converted = convertToCsv(lineWithSpaces);
        assertTrue(converted.length() == 37);

        // FindByCondition
        Map<String, String> kv = getFieldValueMap(lineWithSpaces);
        assertEquals(kv.size(), 3);
        assertTrue(kv.get("key1").equals("value1"));
        assertTrue(kv.get("when").equals("12345678"));

    }

    @org.junit.Test
    public void testEmptyLine() throws Exception {

        String lineWithSpaces = (String) prop.get("emptyLine");
        // Field Format
        List<String> fields = LineParser.getFieldNames(lineWithSpaces);
        assertTrue(fields.size() == 0);

        // GroupByHour
        assertNull(getLong(lineWithSpaces, "when"));

        String converted = convertToCsv(lineWithSpaces);
        assertTrue(converted.length() == 0);

        // FindByCondition
        assertTrue(getFieldValueMap(lineWithSpaces).isEmpty());

    }
}
