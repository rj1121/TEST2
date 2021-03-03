package com.qf.bigdata.realtime.flink.util;


import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class ReflexUtil implements Serializable {


    public static Map<String,Class>  datatypes = new HashMap<String,Class>();
    static {
        datatypes.put(java.lang.Integer.class.getName(),java.lang.Integer.class);
        datatypes.put(java.lang.Long.class.getName(),java.lang.Long.class);
        datatypes.put(java.lang.String.class.getName(),java.lang.String.class);
        datatypes.put(java.lang.Boolean.class.getName(),java.lang.Boolean.class);
        datatypes.put(java.util.List.class.getName(),java.util.List.class);
        datatypes.put(java.util.Map.class.getName(),java.util.Map.class);
    }

    /**
     *
     */
    private static final long serialVersionUID = 1L;


    public static <T> T getInstance(Class<?> cls,Class[] clss,Object[] pars) throws Exception{
        return (T)cls.getConstructor(clss).newInstance(pars);
    }

    public static <T> T getInstance(Class<?> cls) throws Exception{
        return (T)cls.newInstance();
    }


    public static Object getInstance(String clsName) throws Exception{
        Object result = null;
        if(!StringUtils.isEmpty(clsName)){
            result = Class.forName(clsName).newInstance();
        }
        return result;
    }

    public static Map<String,Object> getFildKeyValues(Object obj) throws Exception{
        Map<String,Object> fieldKVs = new HashMap<String,Object>();
        if(null != obj){
            Class cls = obj.getClass();
            Method[] methods = cls.getMethods();
            if(null != methods){
                for(Method method : methods){
                    String methodName = method.getName();
                    if(methodName.startsWith("get") || methodName.startsWith("is") ) {
                        String mName = methodName.replace("get","").replace("is","").toLowerCase();
                        //String mName = methodName.replace("get","").replace("is","");
                        if(!mName.contains("class")){
                            Object value = method.invoke(obj);
                            //System.out.println(mName+","+value);
                            fieldKVs.put(mName,value);
                        }

                    }
                }
            }
        }
        return fieldKVs;
    }



    public static void setFildKeyValues(Object obj, String fieldName, Object fieldValue) throws Exception{
        if(null != obj){
            Class cls = obj.getClass();
            Method[] methods = cls.getMethods();
            if(null != methods){
                for(Method method : methods){
                    String methodName = method.getName();
                    if(methodName.startsWith("set") || methodName.startsWith("is") ) {
                        String mName = methodName.replace("get","").replace("is","").toLowerCase();
                        //String mName = methodName.replace("get","").replace("is","");
                        if(!mName.contains("class") && mName.equalsIgnoreCase(fieldName)){

                            method.invoke(fieldValue);
                        }

                    }
                }
            }
        }
    }


    public static List<String> getFilds(Class cls){
        List<String> fieldNames = new ArrayList();
        if(null != cls){
            Method[] methods = cls.getMethods();
            if(null != methods){
                for(Method method : methods){
                    String methodName = method.getName();
                    if( methodName.startsWith("get") || methodName.startsWith("is") ) {
                        //System.out.println(methodName);
                        String mName = methodName.replace("get","").replace("is","").toLowerCase();
                        if(!mName.contains("class")){
                            System.out.println(mName);
                            fieldNames.add(mName);
                        }
                    }
                }
            }
        }
        return fieldNames;
    }


    /**
     * 静态方法反射执行
     * @param clsName
     * @param methodName
     * @return
     * @throws Exception
     */
    public static Object invoke(String clsName,String methodName) throws Exception{
        Class<?> clazz = Class.forName(clsName);
        Method method = clazz.getMethod(methodName);
        return method.invoke(null);
    }

    public static Object invoke(String clsName,String methodName,String paramCls,Object param) throws Exception{
        //获取类
        Class<?> clazz = Class.forName(clsName);
        //获取参数类型（Map）
        Class<?> pcls = datatypes.getOrDefault(paramCls,java.lang.String.class);
        Method method = clazz.getMethod(methodName,pcls);
        return method.invoke(null,param);
    }


}

