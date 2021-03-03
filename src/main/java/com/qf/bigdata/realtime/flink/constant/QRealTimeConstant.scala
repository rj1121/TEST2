package com.qf.bigdata.realtime.flink.constant


/**
  * 实时场景使用常量
  */
object QRealTimeConstant {

  //===Flink conf================================

  //flink最大乱序时间
  val FLINK_WATERMARK_MAXOUTOFORDERNESS = 5l

  //flink水位间隔时间
  val FLINK_WATERMARK_INTERVAL = 5 * 1000l

  //flink窗口大小
  val FLINK_WINDOW_SIZE :Long = 10L
  val FLINK_WINDOW_MAX_SIZE :Long = 60L

  //flink滑动窗口大小
  val FLINK_SLIDE_WINDOW_SIZE :Long= 10L
  val FLINK_SLIDE_INTERVAL_SIZE :Long= 5L

  //flink检查点间隔
  val FLINK_CHECKPOINT_INTERVAL :Long = 3 * 1000l

  //flink延时设置
  val FLINK_ALLOWED_LATENESS :Long = 10L

  //flink cep
  val FLINK_CEP_VIEW_BEGIN = "CEP_VIEW_BEGIN"

  //本地模型下的默认并行度(cpu core)
  val DEF_LOCAL_PARALLELISM  = Runtime.getRuntime.availableProcessors

  //flink服务重启策略相关参数
  val RESTART_ATTEMPTS :Int = 5
  val RESTART_DELAY_BETWEEN_ATTEMPTS :Long = 1000L * 10L


  //===时间格式================================
  val FORMATTER_YYYYMMDD: String = "yyyyMMdd"
  val FORMATTER_YYYYMMDD_MID: String = "yyyy-MM-dd"
  val FORMATTER_YYYYMMDDHHMMSS_MID: String = "YYYY-MM-dd HH:mm:ss"
  val FORMATTER_YYYYMMDDHH: String = "yyyyMMddHH"
  val FORMATTER_YYYYMMDDHHMMSS: String = "yyyyMMddHHmmss"


  //===mysql属性文件================================
  val JDBC_CONFIG_URL = "jdbc.properties"

  //val JDBC_DRIVER_MYSQL = "com.mysql.jdbc.Driver"
  val JDBC_DRIVER = "jdbc.driver"
  val JDBC_URL = "jdbc.url"
  val JDBC_USERNAME = "jdbc.user"
  val JDBC_PASSWD = "jdbc.password"


  //常数1
  val COMMON_NUMBER_ZERO : Long = Long.box(0l)

  val COMMON_NUMBER_ZERO_INT : Int = Int.box(0)

  val COMMON_NUMBER_ONE : Long = Long.box(1l)

  val COMMON_AGG_ZERO : Double = Double.box(0.0d)

  val COMMON_MAX_COUNT : Long = Long.box(100)


  //===Kafka conf================================
  //kafka消费者配置文件
  val KAFKA_CONSUMER_CONFIG_URL = "kafka/flink/kafka-consumer.properties"
  //kafka生产者配置文件
  val KAFKA_PRODUCER_CONFIG_URL = "kafka/flink/kafka-producer.properties"


  //===Redis conf================================
  val REDIS_CONF_HOST = "redis_host"
  val REDIS_CONF_PASSWORD = "redis_password"
  val REDIS_CONF_TIMEOUT = "redis_timeout"
  val REDIS_CONF_PORT = "redis_port"
  val REDIS_CONF_DB = "redis_db"
  val REDIS_CONF_MAXIDLE = "redis_maxidle"
  val REDIS_CONF_MINIDLE = "redis_minidle"
  val REDIS_CONF_MAXTOTAL = "redis_maxtotal"

  val REDIS_CONF_PATH = "redis/redis.properties"
  val REDIS_DB = 0




}
