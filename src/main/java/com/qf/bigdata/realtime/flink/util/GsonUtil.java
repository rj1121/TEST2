package com.qf.bigdata.realtime.flink.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.LongSerializationPolicy;
import com.qf.bigdata.realtime.flink.until.StreamCC;

import java.util.Map;

/**
 * @Auth dl
 * @Description
 * @Date create at :2020-07-10
 */
public class GsonUtil {

    public static Gson gson = null;
    static {
        GsonBuilder gb = new GsonBuilder().serializeNulls().setDateFormat("yyyy-MM-dd HH:mm:ss");
        gb.setLongSerializationPolicy(LongSerializationPolicy.STRING);
        gson = gb.create();
    }

    /**
     * obj -> json
     * @param obj
     * @return
     */
    public static String gObject2Json(Object obj) {
        String json = gson.toJson(obj, obj.getClass());
        return json;
    }


    public static <T> T gObject2Json(String json,Class<T> classes) {
        return gson.fromJson(json, classes);
    }


    /**
     * json -> map
     * @param str
     * @return
     */
    public static Map<String,Object> gJson2Map(String str) {
        Map<String,Object> result = gson.fromJson(str,Map.class);
        return result;
    }

    /**
     * obj -> json
     * @param obj
     * @return
     */
    public static Map<String,Object> gObject2Map(Object obj) {
        String json = gson.toJson(obj, obj.getClass());
        Map<String,Object> result = gson.fromJson(json,Map.class);
        return result;
    }

    public static void main(String[] args) {
//        String s = "{\"name\":\"37\",\"temp\":94,\"age\":\"1\",\"ct\":\"1596599266742\"}";
//
//        StreamCC.Person pt = gObject2Json(s, StreamCC.Person.class);
//
//
//        System.out.println(pt);
    }

}
