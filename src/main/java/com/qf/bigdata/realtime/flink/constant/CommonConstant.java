package com.qf.bigdata.realtime.flink.constant;

import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class CommonConstant implements Serializable {

    public static final int DEF_NUMBER_ZERO = 0;
    public static final int DEF_NUMBER_ONE = 1;
    public static final int DEF_NUMBER_DUL = 2;

    //用户数量限制级别
    public static final Integer USER_COUNT_LEVEL = 5;

    public static final int DEF_CODE_COUNT = 4; //代码位数
    public static final int DEF_RANGER = 10; //范围

    //时间格式
    public static final DateTimeFormatter PATTERN_YYYYMMDD =  DateTimeFormatter.ofPattern("yyyyMMdd");

    public static final DateTimeFormatter PATTERN_YYYYMMDD_MID =  DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static final DateTimeFormatter PATTERN_HOUR =  DateTimeFormatter.ofPattern("HH");

    public static final String FORMATTER_YYYYMMDD = "yyyyMMdd";
    public static final String FORMATTER_YYYYMMDD_MID = "yyyy-MM-dd";
    public static final String FORMATTER_YYYYMMDDHHMMDD = "yyyyMMddHHmmss";
    public static final String FORMATTER_YYYYMMDDHHMM = "yyyyMMddHHmm";
    public static final String FORMATTER_YYYYMMDDHH = "yyyyMMddHH";

    //发送序列化对象
    public static final ChronoUnit chronoUnit = ChronoUnit.MINUTES;
    public static final ChronoUnit dayChronoUnit = ChronoUnit.DAYS;

    //===charset====================================================================
    public static final String CHARSET_UTF8 = "utf-8"; //测试通道

    //===kafka-topic====================================================================
    public static final String TOPIC_TEST = "t-release"; //投放topic

    public static final String KAFKA_PRODUCER_JSON_PATH = "kafka/kafka-producer.properties";

    //===zk====================================================================
    public static final String ZK_CONNECT = "zk.connect";
    public static final String ZK_CONNECT_KAFKA = "zk.kafka.connect";
    public static final String ZK_SESSION_TIMEOUT = "zk.session.timeout";
    public static final String ZK_CONN_TIMEOUT = "zk.connection.timeout";
    public static final String ZK_BEE_ROOT = "zk.dw.root";


    //===常用符号====================================================================

    public static final String Encoding_UTF8 = "UTF-8";
    public static final String Encoding_GBK = "GBK";

    public static final String MIDDLE_LINE = "-";
    public static final String BOTTOM_LINE = "_";
    public static final String COMMA = ",";
    public static final String SEMICOLON = ";";
    public static final String PLINE = "|";
    public static final String COLON = ":";
    public static final String PATH_W = "\\";
    public static final String PATH_L = "/";
    public static final String POINT = ".";
    public static final String BLANK = " ";

    public static final String LEFT_ARROWS = "<-";
    public static final String RIGHT_ARROWS = "->";

    public static final String LEFT_BRACKET = "[";
    public static final String RIGHT_BRACKET = "]";

    public static final String TAB = "\t";

    //=====================================================
    public static final String KAFKA_DATA_KEY_TOPIC = "topic";
    public static final String KEY_TIMESTAMP = "timestamp";
    public static final String KEY_KEY = "key";
    public static final String KEY_VALUE = "value";
    public static final String KEY_OFFSET = "offset";
    public static final String KEY_PARTITION = "partition";

    public static final String KEY_CTTIME_BEGIN = "ctTimeBegin";
    public static final String KEY_CTTIME_END = "ctTimeEnd";
    public static final String KEY_CTTIME = "ctTime";

}
