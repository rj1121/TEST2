package com.qf.bigdata.realtime.flink.Myutil;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;


public class MyRedisSource implements SourceFunction<HashMap<String,String>> {

    private Logger logger= LoggerFactory.getLogger(MyRedisSource.class);
    private boolean isRunning =true;
    private Jedis jedis=null;
    private final long SLEEP_MILLION=1000;

    @Override
    public void run(SourceContext<HashMap<String, String>> ctx) throws Exception {

        this.jedis = new Jedis("192.168.44.206", 6379);
        while(isRunning){
            try{
//                Thread.sleep(SLEEP_MILLION);
            }catch (JedisConnectionException e){
                logger.warn("redis连接异常，需要重新连接",e.getCause());
                jedis = new Jedis("192.168.44.206", 6379);
            }catch (Exception e){
                logger.warn(" source 数据源异常",e.getCause());
            }
        }
    }

    @Override
    public void cancel() {
        isRunning=false;
        while(jedis!=null){
            jedis.close();
        }
    }


    /**
     * @return
     */
    public int getUV(){

        this.jedis = new Jedis("192.168.44.206", 6379);
        Map<String, String> personName = jedis.hgetAll("PersonName");
        return personName.size();
    }
}
