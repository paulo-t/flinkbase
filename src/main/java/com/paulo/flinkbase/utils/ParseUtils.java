package com.paulo.flinkbase.utils;

public class ParseUtils {
    /**
     *转换为double
     */
    public static double ParseDouble(String str,Double defaultVal){
        try{
            return Double.parseDouble(str);
        }catch (NumberFormatException e){
            return defaultVal;
        }
    }
}
