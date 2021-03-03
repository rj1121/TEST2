package com.qf.bigdata.realtime.flink.util.data.temps;


import com.alibaba.fastjson.JSON;
import com.qf.bigdata.realtime.flink.constant.CommonConstant;
import com.qf.bigdata.realtime.flink.util.CommonUtil;
import com.qf.bigdata.realtime.flink.util.QParameterTool;
import com.qf.bigdata.realtime.flink.util.kafka.KafkaProducerUtil;
import org.apache.commons.lang3.StringUtils;

import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * @Auth dl
 * @Description
 * @Date create at :2020-07-10
 */
public class TempHelper {

    //kafka分区Key
    public static final String KEY_KAFKA_ID = "KAFKA_ID";

    //姓名、年龄、温度
    public static final String KEY_NAME = "name";
    public static final String KEY_AGE = "age";
    public static final String KEY_TEMP = "temp";
    public static final String KEY_CT = "ct";


    /**
     * 模拟人员温度数据
     * @return
     */
    public static Map<String,Object> getTempData(String curTime,Integer count) throws Exception{
        Map<String,Object> result = new HashMap<String,Object>();
        Double d = Math.sqrt(count.doubleValue());

        String name = CommonUtil.getRandom(d.intValue());
        result.put(KEY_NAME, name);

        Integer age = CommonUtil.getRandomNum(2);
        result.put(KEY_AGE, age);

        Integer temp = CommonUtil.getRandomNum(2);
        result.put(KEY_TEMP, temp);

        //创建时间
        Long ct = CommonUtil.getSelectTimestamp(curTime, CommonConstant.FORMATTER_YYYYMMDDHHMMDD);
        result.put(KEY_CT, String.valueOf(ct));

        //hashMD5
        String vid = name.concat(ct.toString());
        String kafkaKey = CommonUtil.getMD5AsHex(vid.getBytes());
        result.put(KEY_KAFKA_ID, kafkaKey);

        return result;
    }

    /**
     * 测试原始数据
     * @param topic
     * @param count
     * @param sleep
     * @throws Exception
     */
    public static void testTempDataForevor(String topic, int count, long sleep) throws Exception{
        //发送序列化对象
        String dateFormatter = CommonConstant.FORMATTER_YYYYMMDDHHMMDD;
        String dayFormatter = CommonConstant.FORMATTER_YYYYMMDD;
        ChronoUnit chronoUnit = ChronoUnit.MINUTES;
        ChronoUnit dayChronoUnit = ChronoUnit.DAYS;

        //时间(天)范围轨迹数据
        while(true){
            String curTime = CommonUtil.formatDate(new Date(), dateFormatter);
            Map<String,String> totalDatas = new HashMap<String,String>();

            for(int y=1; y<count; y++){
                Map<String,Object> data = getTempData(curTime, count);
                String datas = JSON.toJSONString(data);
                System.out.println("ods.data send =" + datas);

                String kafkaKey = data.getOrDefault(KEY_KAFKA_ID,"").toString();
                String dataJson = JSON.toJSONString(data);
                totalDatas.put(kafkaKey, dataJson);
            }
            KafkaProducerUtil.sendMsg(CommonConstant.KAFKA_PRODUCER_JSON_PATH, topic, totalDatas);
            System.out.println("kafka producer send =" + CommonUtil.formatDate4Def(new Date()));
            Thread.sleep(sleep);

            totalDatas.clear();
        }
    }

    /**
     * 选择造数
     * @param args
     * @throws Exception
     */
    public static void chooseFun(String[] args) throws Exception{
        QParameterTool params = QParameterTool.fromArgs(args);

        String topic = params.get(KEY_TOPIC);
        Integer count = params.getInt(KEY_COUNT);
        Integer sleep = params.getInt(KEY_SLEEP);

        testTempDataForevor(topic, count, sleep);
    }

    public static final String KEY_SOURCE = "source";

    public static final String KEY_TOPIC = "topic";
    public static final String KEY_COUNT = "count";
    public static final String KEY_SLEEP = "sleep";

    public static final Integer COUNT_MIN = 0;
    public static final Integer COUNT_MAX = 10000;

    public static final Integer SLEEP_MIN = 1000;
    public static final Integer SLEEP_MAX = 3600 * 1000;


    /**
     * 参数校验
     * @param args
     * @return
     */
    private static String checkParams(String[] args) {
        String result = "";
        QParameterTool params = QParameterTool.fromArgs(args);
        String topic = params.get(KEY_TOPIC);
        if(StringUtils.isEmpty(topic)){
            result = "topic is empty!";
            return result;
        }

        Integer count = params.getInt(KEY_COUNT);
        if(null == count){
            result = "count is empty!";
            return result;
        }else {
            if(count > COUNT_MAX || count < COUNT_MIN){
                result = "count is unbound["+COUNT_MIN+","+COUNT_MAX+"]!";
                return result;
            }
        }
        Integer sleep = params.getInt(KEY_SLEEP);
        if(null == sleep){
            result = "sleep is empty!";
            return result;
        }else {
            if(sleep > SLEEP_MAX || sleep < SLEEP_MIN){
                result = "sleep is unbound["+SLEEP_MIN+","+SLEEP_MAX+"]!";
                return result;
            }
        }

        return result;
    }


    /**
     * 造数入口
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception{

//        String s = "--topic t_flink_temp --count 2 --sleep 1000";
//        String[] args2 = s.split(" ");

        String checkResult = checkParams(args);
        if(StringUtils.isEmpty(checkResult)){
            chooseFun(args);
        }else{
            System.out.println(checkResult);
        }

    }


}
