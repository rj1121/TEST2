package com.qf.bigdata.realtime.flink.util;


import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * 属性文件解析
 */
public class PropertyUtil implements Serializable{

    private static Logger log = LoggerFactory.getLogger(PropertyUtil.class);

    public static final String PROPERTY_FILTER = ".properties";


    /**
     * 读取资源文件
     * @param proPath
     * @return
     */
    public static Properties readProperties(String proPath){
        Validate.notEmpty(proPath, "properties is empty");

        Properties properties = null;
        InputStream is = null;
        try{
            is = PropertyUtil.class.getClassLoader().getResourceAsStream(proPath);
            properties = new Properties();
            properties.load(is);
        }catch(IOException ioe){
            log.error("loadProperties4Redis:" + ioe.getMessage());
        }finally {
            try{
                if(null != is){
                    is.close();
                }}catch (Exception e){
                e.printStackTrace();
            }
        }
        return properties;
    }


    /**
     * 读取资源文件
     * @param proPath
     * @return
     */
    public static Map<String,Object> readProperties2Map(String proPath){
        Validate.notEmpty(proPath, "properties is empty");

        Map<String,Object> result = new HashMap<String,Object>();

        Properties properties = null;
        InputStream is = null;
        try{
            is = PropertyUtil.class.getClassLoader().getResourceAsStream(proPath);
            properties = new Properties();
            properties.load(is);

            Set<Map.Entry<Object,Object>> entrySet = properties.entrySet();
            for(Map.Entry<Object,Object> entry : entrySet){
                String key = entry.getKey().toString();
                Object value = entry.getValue();

                result.put(key, value);
            }

        }catch(IOException ioe){
            log.error("loadProperties4Redis:" + ioe.getMessage());
        }finally {
            try{
                if(null != is){
                    is.close();
                }}catch (Exception e){
                e.printStackTrace();
            }
        }
        return result;
    }


    public static Map<String,String> readProperties2Map4String(String proPath){
        Validate.notEmpty(proPath, "properties is empty");

        Map<String,String> result = new HashMap<String,String>();

        Properties properties = null;
        InputStream is = null;
        try{
            is = PropertyUtil.class.getClassLoader().getResourceAsStream(proPath);
            properties = new Properties();
            properties.load(is);

            Set<Map.Entry<Object,Object>> entrySet = properties.entrySet();
            for(Map.Entry<Object,Object> entry : entrySet){
                String key = entry.getKey().toString();
                String value = entry.getValue().toString();

                result.put(key, value);
            }

        }catch(IOException ioe){
            log.error("loadProperties4Redis:" + ioe.getMessage());
        }finally {
            try{
                if(null != is){
                    is.close();
                }}catch (Exception e){
                e.printStackTrace();
            }
        }
        return result;
    }





}
