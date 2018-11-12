package com.datageek.main;

import com.datageek.bdr.dao.*;
import com.datageek.bdr.dao.factory.SparkDaoFactory;
import com.datageek.bdr.dao.util.Constant.DBType;
import com.datageek.bdr.util.PropertiesUtils;
import com.datageek.bdr.util.SaveLogUtils;
import com.datageek.etl.Etl2Json;
import com.datageek.etl.lib.model.Column;
import com.datageek.etl.lib.model.ProcessorList;
import com.datageek.etl.lib.model.Schema;
import com.datageek.jdbcDao.MysqlJDBCDao;
import kafka.serializer.StringDecoder;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;
@Deprecated
public class ETLWithKafkaNew implements Serializable {
    private static final long serialVersionUID = 1L;
    private final static Log log = LogFactory.getLog(ETLWithKafkaNew.class);
    private static boolean config_db = true;
    private static boolean use_es = false;
    private static int partitions = 3;
    private static int interval = 5;
    private static boolean reset_kafka = false;
    private static String jobName = "";

    public static void main(String[] args) throws Exception {
        Options opt = new Options();
        opt.addOption("b", "kafka-brokers", true, "the brokers list of kafka");
        opt.addOption("d", "dry-run", false, "whether execute this process with dry_run");
        opt.addOption("e", "use-esdao", false, "whether use esdao to save");
        opt.addOption("f", "json-file", true, "the json file which contains patterns");
        opt.addOption("h", "help", false, "help message");
        opt.addOption("i", "interval", true, "the batch interval of streaming");
        opt.addOption("k", "kafka-topic", true, "the topic of kafka");
        opt.addOption("n", "name", true, "the job unique name(id)");
        opt.addOption("p", "partitions", true, "the number of partitions");
        opt.addOption("r", "reset-kafka-offset", false, "consume kafka topic from beginning");
        opt.addOption("t", "type", true, "the device type of log");
        opt.addOption("u", "use-config-file", false, "whether read all configures from file");


        boolean dryRun = false;
        String topicName = "";
        String brokers = "";
        String sourceName = "";
        String configFilePath = "";

        String formatstr = "sh run.sh yarn-cluster|yarn-client|local ....";

        HelpFormatter formatter = new HelpFormatter();
        CommandLineParser parser = new PosixParser();
        CommandLine cl = null;
        try {
            cl = parser.parse(opt, args);
        } catch (ParseException e) {
            formatter.printHelp(formatstr, opt);
            System.exit(1);
        }
        if (cl.hasOption("b")) {
            brokers = cl.getOptionValue("b");
        }
        if (cl.hasOption("d")) {
            dryRun = true;
        }
        if (cl.hasOption("e")) {
            use_es = true;
        }
        if (cl.hasOption("f")) {
            configFilePath = cl.getOptionValue("f");
        }
        if (cl.hasOption("h")) {
            formatter.printHelp(formatstr, opt);
            System.exit(0);
        }
        if (cl.hasOption("i")) {
            interval = Integer.parseInt(cl.getOptionValue("i"));
        }
        if (cl.hasOption("k")) {
            topicName = cl.getOptionValue("k");
        }
        if (cl.hasOption("n")) {
            jobName = cl.getOptionValue("n");
        }
        if (cl.hasOption("p")) {
            partitions = Integer.parseInt(cl.getOptionValue("p"));
        }
        if (cl.hasOption("r")) {
            reset_kafka = true;
        }
        if (cl.hasOption("t")) {
            sourceName = cl.getOptionValue("t");
        }
        if (cl.hasOption("u")) {
            config_db = false;
        }

        log.info("dryRun: " + dryRun);
        log.info("config_db: " + config_db);
        log.info("topic name: " + topicName);
        log.info("brokers quorum: " + brokers);
        log.info("source name: " + sourceName);
        log.info("partitions: " + partitions);
        log.info("config file path: " + configFilePath);
        log.info("use_es: " + use_es);
        log.info("reset_kafka: " + reset_kafka);
        log.info("job_name: " + jobName);

        //String brokersList = "172.31.18.12:9092,172.31.18.13:9092,172.31.18.14:9092";
        //String topic = "whsh";
        SaveLogUtils savelog = new SaveLogUtils(brokers, jobName, "ETLWithKafkaNew[" + sourceName + "]");

        HashMap<String, Schema> etlSchema = Etl2Json.getSchemas(sourceName, configFilePath, config_db);
        ProcessorList processotlist = Etl2Json.getProcessorList(sourceName, configFilePath, config_db);

        HashMap<String, String> sqls = new HashMap<>();
        for (Map.Entry<String, Schema> entry : etlSchema.entrySet()) {
            Column[] columns = entry.getValue().getColumns();
            String sql = "";
            for (Column column : columns) {
                sql += column.getColumnName() + ",";
            }
            sqls.put(entry.getKey(), sql.substring(0, sql.length() - 1));
        }

        process(topicName, brokers, sourceName, etlSchema, processotlist, dryRun, sqls, savelog);
    }

    private static void process(String topicName, String brokers, String sourceName,
                                HashMap<String, Schema> etlSchema, ProcessorList processotlist,
                                boolean dryRun, HashMap<String, String> sqls, SaveLogUtils savelog)
            throws IllegalArgumentException, IOException {
        String CONSUMER_GROUP_ID = "ETLWithKafkaNew";
        savelog.write("start", "success", "Started..");
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf sparkConf = new SparkConf().setAppName(CONSUMER_GROUP_ID + "[" + sourceName + "]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        MysqlJDBCDao mysqljdbcDao = new MysqlJDBCDao();
        if (use_es) {
            String nodes = "";
            String port = "";
            if (!config_db) {
                try {
                    PropertiesUtils.loadPropertiesByClassPath("conn.properties");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                nodes = PropertiesUtils.getProperty("db.es.nodes");
                port = PropertiesUtils.getProperty("db.es.port");
            } else {
                try {
                    HashMap<String, String> es = mysqljdbcDao.getConfig("esdao");
                    nodes = es.get("db.es.nodes");
                    port = es.get("db.es.port");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            sparkConf.set("pushdown", "true");
            sparkConf.set("es.nodes", nodes);
            sparkConf.set("es.port", port);
            sparkConf.set("es.index.auto.create", "true");
        }


        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(topicName, 1);

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(interval * 1000));

        savelog.write("initialize", "success", "applictionid = " + jssc.ssc().sparkContext().applicationId());
        log.info("=========applictionid = " + jssc.ssc().sparkContext().applicationId());

        String timeStamp = String.valueOf(System.currentTimeMillis());
        //mysqljdbcDao.save(jobName, CONSUMER_GROUP_ID + "[" + sourceName + "]", sparkConf.getAppId(), timeStamp);
        mysqljdbcDao.save(jobName, jssc.ssc().sparkContext().applicationId(),
                CONSUMER_GROUP_ID + "[" + sourceName + "]" , timeStamp);


        try {
            mysqljdbcDao.closeConn();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        HiveContext hContext = new HiveContext(jssc.sparkContext());
        hContext.setConf("hive.server2.thrift.port", "10000");

        //JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum,
        //CONSUMER_GROUP_ID, topicMap);
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topicName.split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        if (reset_kafka) {
            kafkaParams.put("auto.offset.reset", "smallest");
        }

        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc, String.class, String.class, StringDecoder.class, StringDecoder.class,
                kafkaParams, topicsSet);

        JavaDStream<HashMap<String, JSONObject>> parsedLogDStream = messages.repartition(partitions).map(message -> {
            //JavaDStream<HashMap<String, JSONObject>> parsedLogDStream = messages.map(message -> {
            String line = message._2;
            HashMap<String, JSONObject> jsonObject = null;
            try {
                jsonObject = Etl2Json.etl_new(line, etlSchema, processotlist);
            } catch (Exception e) {
                log.error(e.getMessage());
                savelog.write("ETL2Json","error",e.getMessage());
                e.printStackTrace();
            }
            return jsonObject;
        });

        boolean needFilter = true;
        String typeWithAllData = "";
        for (Map.Entry<String, Schema> entry : etlSchema.entrySet()) {
            if (entry.getValue().getAllData()) {
                typeWithAllData = entry.getKey();
                needFilter = false;
                break;
            }
        }
        JavaDStream<HashMap<String, JSONObject>> filter;
        //if (needFilter) {
        filter = parsedLogDStream.filter(json -> json != null);
        //} else {
        //    filter = parsedLogDStream;
        //}

        if (etlSchema.keySet().size() > 1) {
            filter.cache();
        }

        if (filter != null) {
            boolean finalNeedFilter = needFilter;
            final String finalTypeWithAllData = typeWithAllData;
            filter.foreachRDD(parsedLogRdd -> {
                log.info("=========partition number is " + parsedLogRdd.getNumPartitions());
                if (!parsedLogRdd.isEmpty()) {
                    if (dryRun) {
                        log.info("==========dryrun starts...");
                        if (finalNeedFilter) {
                            log.info("=========no schema contains all records, please set all_data as true in one schema...");
                        } else {
                            JavaRDD<JSONObject> typeWithAll = parsedLogRdd.map(m -> m.get(finalTypeWithAllData));
                            long total = typeWithAll.count();
                            JavaRDD<JSONObject> failed = typeWithAll.filter(json -> json.getString("id").equals(""));
                            long failCount = failed.count();
                            failed.take(20).forEach(a -> log.info("=====parsed failed:" + a.toString()));
                            log.info("======ETL patterns' parsed rate is :" + String.format("%.2f", ((total - failCount) / (double) total) * 100)
                                    + "%, total records' count=" + total + ", failed records' count=" + failCount);
                        }
                        log.info("============================Dryrun is finished in one sparkStreaming batch interval......");
                    } else {
                        for (String key : etlSchema.keySet()) {
                            log.info("####################key =" + key);
                            savelog.write("ETL:RESFILTER","running","key =" + key);
                            JavaRDD<JSONObject> a = parsedLogRdd.map(m -> m.get(key));
                            a.cache();
                            if (a != null && !a.isEmpty()) {
                                JavaRDD<String> jsonrdd = a.filter(json -> json != null).map(JSONObject::toString);
                                DataFrame dfo = hContext.read().json(jsonrdd);
                                log.info("###########the columns are:" + sqls.get(key));
                                DataFrame df = dfo.selectExpr(sqls.get(key).split(","));
                                savelog.write("ETL:toDF","succeed","The dataFrame is got from json rdd");
                                //df.show();
                                if (df != null && df.columns().length > 3) {
                                    log.info("====================key = " + key);
                                    // hiveDao
                                    if (etlSchema.get(key).getDbType().trim().equalsIgnoreCase("hive")) {
                                        log.info("used hivedao to save is starting....");
                                        savelog.write("ETL:hivedao","running","used hivedao to save is starting....");
                                        HiveDao sparkDao = (HiveDao) SparkDaoFactory.createDao(DBType.HIVE);
                                        log.info("===================================table name = "
                                                + etlSchema.get(key).getDbName() + "." + etlSchema.get(key).getTableName());
                                        savelog.write("ETL:hivedao","running","table name = "
                                                + etlSchema.get(key).getDbName() + "." + etlSchema.get(key).getTableName());
                                        sparkDao.save(hContext, df,
                                                etlSchema.get(key).getDbName() + "." + etlSchema.get(key).getTableName());
                                        log.info("used hivedao to save is finished .....");
                                        savelog.write("ETL:hivedao","succeed","used hivedao to save is starting....");
                                    }
                                    // mysqlDao
                                    else if (etlSchema.get(key).getDbType().trim().equalsIgnoreCase("mysql")) {
                                        log.info("used mysqldao to save is starting.....");
                                        MysqlDao sparkDao = (MysqlDao) SparkDaoFactory.createDao(DBType.MYSQL);
                                        log.info(
                                                "===================================table name = " + etlSchema.get(key).getTableName());
                                        sparkDao.save(hContext, df, etlSchema.get(key).getTableName());
                                        log.info("used mysqldao to save is finished.....");
                                    }
                                    // oracleDao
                                    else if (etlSchema.get(key).getDbType().trim().equalsIgnoreCase("oracle")) {
                                        log.info("used oracledao to save is starting.....");
                                        OracleDAO sparkDao = (OracleDAO) SparkDaoFactory.createDao(DBType.ORACLE);
                                        log.info(
                                                "===================================table name = " + etlSchema.get(key).getTableName());
                                        sparkDao.save(hContext, df, etlSchema.get(key).getTableName());
                                        log.info("used oracledao to save is finished.....");
                                    }
                                    // sdbDao
                                    else if (etlSchema.get(key).getDbType().trim().equalsIgnoreCase("sdb")) {
                                        log.info("used sdbdao to save is starting.....");
                                        SDBDao sparkDao = (SDBDao) SparkDaoFactory.createDao(DBType.SDB);
                                        log.info(
                                                "===================================table name = " + etlSchema.get(key).getTableName());
                                        sparkDao.save(hContext, df, etlSchema.get(key).getTableName());
                                        log.info("used sdbdao to save is finished.....");
                                    }
                                    // ESDao
                                    else if (etlSchema.get(key).getDbType().trim().equalsIgnoreCase("es")) {
                                        log.info("used esdao to save is starting.....");
                                        if (!use_es) {
                                            log.info("used esdao is not supported by input setting.....");
                                        } else {
                                            ESDao sparkDao = (ESDao) SparkDaoFactory.createDao(DBType.ES);
                                            log.info(
                                                    "===================================table name = " + etlSchema.get(key).getTableName());
                                            sparkDao.save(hContext, df, etlSchema.get(key).getTableName());
                                        }
                                        log.info("used esdao to save is finished.....");
                                    }
                                    // others
                                    else {
                                        log.info("\n===================================the database used to save is not support now!");
                                        savelog.write("ETL:hivedao","failed","the database used to save is not support now!");
                                    }
                                } else {
                                    log.info("\n===================================the df is empty, no needing to save!");
                                    savelog.write("ETL:SAVE","failed","the df is empty, no needing to save!");
                                }
                            } else {
                                log.info("\n===================================the jsonrdd is empty, no needing to save!");
                                savelog.write("ETL:toDF","failed","the jsonrdd is empty, no needing to df!");
                            }
                        }
                    }
                } else {
                    log.info("\n===================================the parsedLogRdd is empty, nothing got from ETL...");
                    savelog.write("ETL:RESFILTER","failed","the parsedLogRdd is empty, nothing got from ETL...");
                }

            });
        } else {
            log.info("filter is empty.....");
        }
        jssc.start();
        jssc.awaitTermination();
    }
}