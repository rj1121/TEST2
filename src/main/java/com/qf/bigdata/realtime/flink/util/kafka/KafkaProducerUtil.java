package com.qf.bigdata.realtime.flink.util.kafka;


import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * 消息生产者工具类
 */
public class KafkaProducerUtil {

    private final static Logger log = LoggerFactory.getLogger(KafkaProducerUtil.class);


    /**
     * 创建生产者
     * @return
     */
    public static KafkaProducer<String, String> createProducer(String path) throws IOException {
        Validate.notEmpty(path, "path must be not empty");
        KafkaProducer<String, String>  producer = null;
        Properties props = new Properties();
        props.load(KafkaProducerUtil.class.getClassLoader().getResourceAsStream(path));
        if(null != props){
            producer = new KafkaProducer<String, String>(props);
        }
        return producer;
    }


    /**
     * json数据
     * @param path
     * @param topic
     * @param datas
     */
    public static void sendMsg(String path, String topic, Map<String,String> datas) throws Exception{
        Validate.notEmpty(path, "kafka config path is not empty");
        Validate.notEmpty(topic, "topic is not empty");
        Validate.notNull(datas, "datas is not empty");

        KafkaProducer producer = createProducer(path);
        if(null != producer){
            List<String> lines = new ArrayList<String>();
            for(Map.Entry<String, String> entry : datas.entrySet()){
                String key = entry.getKey();
                String value = entry.getValue();
                log.info("Producer.key=" + key + ",value=" + value);

                producer.send(new ProducerRecord<String, String>(topic, key, value));
                producer.flush();
            }
            producer.close();
        }

    }



    //==========================================================================
    public static void main(String[] args) throws  Exception {





    }
}
