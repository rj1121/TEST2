package com.qf.bigdata.realtime.flink.util.data;


import com.qf.bigdata.realtime.flink.constant.CommonConstant;
import com.qf.bigdata.realtime.flink.util.CommonUtil;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

/**
 * @Auth dl
 * @Description
 * @Date create at :2020-07-09
 */
public class PersonDataHelper implements Serializable {

    /**
     * 姓名
     */
    public static String name(Map<String,Object> values){
        String name = CommonUtil.getRandomChar(4);
        return name;
    }

    /**
     * 年龄
     */
    public static Integer age(Map<String,Object> values){
        Integer age = CommonUtil.getRandomNum(2);
        return age;
    }

    /**
     * 温度
     */
    public static Integer temp(Map<String,Object> values){
        Integer temp = CommonUtil.getRandomNum(2);
        return temp;
    }

    /**
     * 时间
     */
    public static Long ct(Map<String,Object> values){
        String curTime = CommonUtil.formatDate(new Date(), CommonConstant.FORMATTER_YYYYMMDDHHMMDD);
        Long ct = CommonUtil.getSelectTimestamp(curTime, CommonConstant.FORMATTER_YYYYMMDDHHMMDD);
        return ct;
    }


}
