package com.datageek.main;

import com.datageek.bdr.dao.HiveDao;
import com.datageek.jdbcDao.MysqlJDBCDao;
import com.datageek.bdr.dao.factory.SparkDaoFactory;
import com.datageek.bdr.dao.util.Constant;
import com.datageek.bdr.util.PropertiesUtils;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;

import static org.apache.spark.sql.functions.callUDF;

/**
 * 处理json格式日志，可以使用事先规定好json key和hive column的映射关系
 * 实现对json日志的抽取入库
 */
@Deprecated
public class ETLWithFile4Json2 implements Serializable {
    private static final long serialVersionUID = 1L;
    private final static Log log = LogFactory.getLog(ETLWithFile4Json2.class);
    private static boolean config_db = true;
    private static boolean use_es = false;

    public static class AddID extends scala.runtime.AbstractFunction0<String> implements Serializable{
        public String apply() {
            return UUID.randomUUID().toString();
        }
    }
    public static class AddTimeStamp extends scala.runtime.AbstractFunction0<String> implements Serializable {
        public String apply() {
            return String.valueOf(System.currentTimeMillis());
        }
    }

    public static void main(String[] args) throws Exception {
        Options opt = new Options();
        opt.addOption("d", "dry-run", false, "whether execute this process with dry-run");
        opt.addOption("p", "path", true, "the path of log file, default in HDFS");
        opt.addOption("t", "type", true, "the device type of log");
        opt.addOption("h", "help", false, "help message");
        opt.addOption("u", "use-config-file", false, "whether read all configures from file");
        opt.addOption("s", "save-table", true, "the table used as saving");
        opt.addOption("e", "use-esdao", false, "whether use esdao to save");

        boolean dryRun = false;
        String logFilePath = "";
        String sourceName = "";
        String tableName = "";

        String formatstr = "sh run.sh yarn-cluster|yarn-client|local [-d/--dryrun] -p/--path <pathvalue> -t/--type <logtype> -f/--jsonfile <jsonfilename>";

        HelpFormatter formatter = new HelpFormatter();
        CommandLineParser parser = new PosixParser();
        CommandLine cl = null;
        try {
            cl = parser.parse(opt, args);
        } catch (ParseException e) {
            formatter.printHelp(formatstr, opt);
            System.exit(1);
        }
        if (cl.hasOption("h")) {
            formatter.printHelp(formatstr, opt);
            System.exit(0);
        }
        if (cl.hasOption("d")) {
            dryRun = true;
        }
        if (cl.hasOption("u")) {
            config_db = false;
        }
        if (cl.hasOption("p")) {
            logFilePath = cl.getOptionValue("p");
        }
        if (cl.hasOption("t")) {
            sourceName = cl.getOptionValue("t");
        }
        if (cl.hasOption("s")) {
            tableName = cl.getOptionValue("s");
        }
        if (cl.hasOption("e")) {
            use_es = true;
        }

        log.info("dryRun: " + dryRun);
        log.info("config_db: " + config_db);
        log.info("log file path: " + logFilePath);
        log.info("source name: " + sourceName);
        log.info("save table name : " + tableName);
        log.info("use_es: " + use_es);

        //HashMap<String, Schema> etlSchema = Etl2Json.getSchemas(sourceName, configFilePath, config_db);
        //ProcessorList processotlist = Etl2Json.getProcessorList(sourceName, configFilePath, config_db);

        process(logFilePath, sourceName, tableName, dryRun);
    }

    private static void process(String logFilePath, String sourceName, String tableName, boolean dryRun)
            throws IllegalArgumentException, IOException {
        String CONSUMER_GROUP_ID = "EtlWithFile4Json2";
        SparkConf sparkConf = new SparkConf().setAppName(CONSUMER_GROUP_ID + "[" + sourceName + "]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        if(use_es) {
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
                MysqlJDBCDao mysqljdbcDao = new MysqlJDBCDao();
                try {
                    HashMap<String, String> es = mysqljdbcDao.getConfig("esdao");
                    nodes = es.get("db.es.nodes");
                    port = es.get("db.es.port");
                    mysqljdbcDao.closeConn();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            sparkConf.set("pushdown", "true");
            sparkConf.set("es.nodes", nodes);
            sparkConf.set("es.port", port);
            sparkConf.set("es.index.auto.create", "true");
        }

        String jsonColumnMap = "";
        if (!config_db) {
            try {
                PropertiesUtils.loadPropertiesByClassPath("conn.properties");
            } catch (IOException e) {
                e.printStackTrace();
            }
            jsonColumnMap = PropertiesUtils.getProperty("json.column.map");
        } else {
            MysqlJDBCDao mysqljdbcDao = new MysqlJDBCDao();
            try {
                HashMap<String, String> es = mysqljdbcDao.getConfig("json");
                jsonColumnMap = mkString(es);
                mysqljdbcDao.closeConn();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        log.info("###################jsonColumnMap = " + jsonColumnMap);

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        HiveContext hContext = new HiveContext(sc);

        //JavaRDD<String> lines = sc.textFile(logFilePath);
        DataFrame df = hContext.read().json(logFilePath);

        /*hContext.udf().register("addTimeStamp", new UDF1<Long, String>() {
            @Override
            public String call(Long l) throws Exception {
                return String.valueOf(l);
            }
        }, DataTypes.StringType);

        hContext.udf().register("addUUID", new UDF1<String, String>() {
            @Override
            public String call(String s) throws Exception {
                UUID uuid = UUID.fromString(s);
                return uuid.toString();
            }
        }, DataTypes.StringType);*/

        //df.withColumn("time_stamp", callUDF("addTimeStamp", System.currentTimeMillis()));
        DataFrame df1 = df.withColumn("time_stamp", callUDF(new AddTimeStamp(), DataTypes.StringType));
        DataFrame df2 = df1.withColumn("id", callUDF(new AddID(), DataTypes.StringType));
        DataFrame dfRes = df2.selectExpr(jsonColumnMap.split(","));

        HiveDao sparkDao = (HiveDao) SparkDaoFactory.createDao(Constant.DBType.HIVE);
        log.info("===================================table name = " + tableName);
        try {
            sparkDao.save(hContext, dfRes, tableName);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
        log.info("used hivedao to save is finished .....");

        sc.close();
        sc.stop();
    }

    public static String mkString(HashMap<String, String> es){
        String str = "";
        Object[] key_arr = es.keySet().toArray();
        Arrays.sort(key_arr);
        for  (Object key : key_arr) {
            Object value = es.get(key);
            str += key + " as " + value + ",";
        }
        /*for(Map.Entry<String, String> entry:es.entrySet()){
            //System.out.println(entry.getKey()+"--->"+entry.getValue());
            str += entry.getKey() + " as " + entry.getValue() + ",";
        }*/
        return str.substring(0,str.length()-1);
    }
}
