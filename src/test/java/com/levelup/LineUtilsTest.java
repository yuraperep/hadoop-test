package com.levelup;

import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
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

    @Before
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
    public void testLineWithInnerObjects() throws Exception {

        String lineWithInnerObj = (String) prop.get("lineWithInnerObj");
        Map<String, String> kv = getKVwithInnerObjectsMap(lineWithInnerObj);
        assertTrue(kv.size() == 7);
        assertTrue(kv.get("_id_time").equals("1382127311000"));

        lineWithInnerObj = (String) prop.get("lineWithInnerObj2");
        kv = getKVwithInnerObjectsMap(lineWithInnerObj);
        assertTrue(kv.size() == 9);
        assertTrue(kv.get("_id2_time").equals("1382127311000"));

    }

    @Test
    public void testEmptyLine() throws Exception {

        String emptyLine = (String) prop.get("emptyLine");

        Map<String, String> kv = getFieldValueMap(emptyLine);
        assertTrue(kv.isEmpty());
        String converted = convertToCsv(kv);
        assertTrue(converted.length() == 0);
    }

    @Test
    public void testMongoExportLine() throws Exception {

        String line = (String) prop.get("mongo.export.line");
        Map<String, String> kv = getKVwithInnerObjectsMap(line);
        assertTrue(kv.size() == 6);

        line = (String) prop.get("mongo.export.line2");
        kv = getKVwithInnerObjectsMap(line);
        assertTrue(kv.size() == 7);

    }

    @Test
    public void testMongoExportAdvancedCases() throws Exception {

        String line = (String) prop.get("mongo.export.line3");
        Map<String, String> kv = getKVwithInnerObjectsMap(line);
        assertTrue(kv.size() == 8);

        line = (String) prop.get("mongo.export.lineUpload");
        kv = getKVwithInnerObjectsMap(line);
        assertTrue(kv.size() == 7);



    }

    @Test
    public void testTimestampFromHexOid() throws Exception {

        String hexOid = "52617b7b26f4b3c367073fa9";
        long time = getTimestampAsLongFromHexOid(hexOid);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd.H'h'");
        String dateTime = df.format(new Date(time));
        assertTrue(dateTime.equals("2013-10-18.21h"));


        dateTime = getWeekAsFormattedDate(hexOid);
        assertTrue(dateTime.equals("2013-10-17"));


    }

    @Test
    public void testGetPathFromURL() throws Exception {

        assertTrue(getPathFromURL("http://ya.ru").equals("ya.ru"));
        assertTrue(getPathFromURL("http://ad2.leonbets.com/www/delivery/afr.php?zoneid=57&target=_blank&cb={random}&ct0={clickurl}")
                .equals("ad2.leonbets.com/www/delivery/afr.php"));
    }
}
