package com.datageek.main;

import com.datageek.bdr.batch.SparkBatch;
import com.datageek.bdr.util.MysqlUtils;
import com.datageek.bdr.util.SaveLogUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * 按照2017-01-05版WEB页面设计好的批处理程序，具备使用SavelogUtil存储日志功能
 * 能够处理多个sql，存入多个物理表，而不是只有最后一个存物理表
 */
public class SparkBatchMainNew {
    private final static Log log = LogFactory.getLog(SparkBatchMainNew.class);
    private static String CONSUMER_GROUP_ID = "SparkBatchMainNew";
    private static boolean config_db = true;
    private static boolean use_es = false;
    private static boolean without_limit = false;
    private static String taskUUID = "";

    public static void main(String[] args) throws Exception {
        /*if (args.length < 1) {
            log.error("请传递一个参数：所要执行批处理的类别");
			System.exit(-1);
		}*/

        String[] args0 = args[0].split(",");
        String name = args0[0];
        if (args0.length > 1) {
            taskUUID = args0[1];
        }
        String format = "orc";
        String mode = "append";
        without_limit = true;
        String type = "BatchJobs:" + name;

        SaveLogUtils savelog = new SaveLogUtils(name, type);

        log.info("name: " + name);
        log.info("taskUUID: " + taskUUID);
        log.info("isUsingConfigFile: " + config_db);
        log.info("use_es: " + use_es);
        log.info("without_limit: " + without_limit);
        log.info("format: " + format);
        log.info("mode: " + mode);

        SparkBatch sparkbatch = new SparkBatch(format, mode);

        SparkConf sparkConf = new SparkConf().setAppName(CONSUMER_GROUP_ID + name);
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        HiveContext hContext = new HiveContext(sc);


        String applicationId = sc.sc().applicationId();
        log.info("application_id = " + applicationId);

        if (args0.length > 1) {
            MysqlUtils.saveApplicationIDToTaskBatch(applicationId, taskUUID);
        } else {
            MysqlUtils.saveApplicationID4Batch(applicationId, name);
        }

        process(sparkbatch, name, savelog, hContext);

        savelog.close();
    }

    private static void toSave(SparkBatch sparkbatch, JavaSparkContext sc, HiveContext hContext, String jsonString) {
        List<String> data = new ArrayList<>();
        data.add(jsonString);
        JavaRDD<String> jsonData = sc.parallelize(data);
        DataFrame df = hContext.read().json(jsonData);
        sparkbatch.saveToMysql(hContext, df, "batch_job_status");
    }

    private static String longtodate(long time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  //MM/dd/yyyy HH:mm:ss
        Date dt = new Date(time);
        return sdf.format(dt);
    }

    private static ArrayList<String> changeTableName(ArrayList<String> tableNames, String newDbName) {
        ArrayList<String> ress = new ArrayList<>();
        tableNames.forEach(r -> {
            if (!r.contains(".")) {
                String res = newDbName + "." + r;
                ress.add(res);
            } else {
                ress.add(r);
            }
        });
        return ress;
    }

    public static void process(SparkBatch sparkbatch, String name, SaveLogUtils savelog, HiveContext hContext)
            throws IllegalArgumentException, IOException {
        long startTime = System.currentTimeMillis();
        ArrayList<String> sqlList = new ArrayList<>();
        ArrayList<String> mode = new ArrayList<>();
        ArrayList<String> dbType = new ArrayList<>();
        ArrayList<String> tableName = new ArrayList<>();

        String id;
        if (name.contains("@2@")) {
            id = name.split("@2@")[1];
        } else {
            id = name;
        }
        String dbName = "";
        synchronized (SparkBatchMainNew.class) {
            try {
                HashMap<String, ArrayList<String>> res = MysqlUtils.getBatchConfigByUUID(id);
                dbName = MysqlUtils.getDicValue("DAO_API_SCHEMA", "DAO_API_SCHEMA", "DAO_API_SCHEMA");
                sqlList = res.get("sqlList");
                mode = res.get("mode");
                dbType = res.get("dbType");
                tableName = res.get("tableName");
                /*if (dbName.equalsIgnoreCase("")) {
                    tableName = res.get("tableName");
                } else {
                    tableName = changeTableName(res.get("tableName"), dbName);
                }*/
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        Logger.getLogger("org").setLevel(Level.ERROR);
        savelog.write("start", "success", "Started..");

        String warningUUID ="";
        try {
            warningUUID = MysqlUtils.getWarningUUIDByBatchUUID(id);
        } catch (Exception e) {
            e.printStackTrace();
        }

        log.info("####warningUUID = "+ warningUUID);
        int ret = sparkbatch.run(hContext, sqlList, mode, dbType, tableName, savelog, dbName, warningUUID);
        long endTime = System.currentTimeMillis();
        long timeOccupy = endTime - startTime;
        log.info("spark batch job is finished, Occupying time is " + timeOccupy + "ms, status is " + (ret == 0 ? "success." : "fail."));
        savelog.write("结束", "成功", "spark batch job is finished, Occupying time is " + timeOccupy + "ms, status is " + (ret == 0 ? "success." : "fail."));
        if (!name.trim().equalsIgnoreCase("")) {
            String str = "{\"pid\":\"" + name + "\",\"start_time\":\"" + longtodate(startTime)
                    + "\",\"end_time\":\"" + longtodate(endTime) + "\",\"time_occupy\":\"" +
                    timeOccupy + "\",\"status\":\"";
            if (ret == 0) {
                str += "success\"}";
            } else {
                str += "fail\"}";
            }
            log.info("json str: " + str);
            //savelog.write("whsh", type, String.valueOf(id), str);
            //toSave(sparkbatch, sc, hContext, str);
        }
    }
}
