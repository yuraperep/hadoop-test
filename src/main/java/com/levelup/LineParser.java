package com.levelup;

/**
 * Created with IntelliJ IDEA.
 * User: perep80
 * Date: 11/16/13
 * Time: 2:32 PM
  */
public class LineParser {

    public static String getString(String line,String fieldName) {
        int fieldStart = line.indexOf(fieldName);
        if(fieldStart>-1)
            return line.substring(fieldStart+fieldName.length()+3,line.indexOf(",",fieldStart)-1);
        else
            return null;
    }

    public static Integer getInt(String line, String fieldName) {
        try{
            String res = getString(line,fieldName);
            if(res!=null)
                return Integer.parseInt(res);
            else
                return null;
        }catch(Exception e){
            return null;
        }
    }

    public static Long getLong(String line, String fieldName) {
        try{
            String res = getString(line,fieldName);
            if(res!=null)
                return Long.parseLong(res);
            else
                return null;
        }catch(Exception e){
            return null;
        }
    }

    public static String convertToCsv(String line) {
        return line.substring(line.lastIndexOf("{")+1,line.indexOf("}")).replaceAll("\"[a-z0-9]+\":","");
    }

    public static String getFieldNamesString(String line) {
        return line.substring(line.lastIndexOf("{")+1,line.indexOf("}")).replaceAll(":\"[a-z0-9.]+\"","");
    }

}
