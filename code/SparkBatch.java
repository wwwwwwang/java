package com.datageek.bdr.batch;

import com.datageek.bdr.dao.*;
import com.datageek.bdr.dao.factory.SparkDaoFactory;
import com.datageek.bdr.dao.util.Constant;
import com.datageek.bdr.dao.util.Constant.DBType;
import com.datageek.bdr.util.SaveLogUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import java.util.ArrayList;

public class SparkBatch {

    private static Log log = LogFactory.getLog(SparkBatch.class);
    private Constant.Format format = Constant.Format.ORC;
    private Constant.SaveModes mode = Constant.SaveModes.APPEND;

    public SparkBatch(String f, String m) {
        if (!f.equalsIgnoreCase("") &&
                (f.equalsIgnoreCase(Constant.Format.ORC.getName())
                        || f.equalsIgnoreCase(Constant.Format.JDBC.getName())
                        || f.equalsIgnoreCase(Constant.Format.JSON.getName())
                        || f.equalsIgnoreCase(Constant.Format.PARQUET.getName())
                        || f.equalsIgnoreCase(Constant.Format.TABLE.getName())
                        || f.equalsIgnoreCase(Constant.Format.TEXT.getName()))) {
            format.setName(f);
        }
        if (!m.equalsIgnoreCase("") && (
                m.equalsIgnoreCase(Constant.SaveModes.APPEND.getName())
                        || m.equalsIgnoreCase(Constant.SaveModes.ERROR.getName())
                        || m.equalsIgnoreCase(Constant.SaveModes.IGNORE.getName())
                        || m.equalsIgnoreCase(Constant.SaveModes.OVERWRITE.getName())
        )) {
            mode.setName(m);
        }
    }


    public int run(HiveContext hContext, String sql, String DBtype, String tablename, boolean withoutLimit) {
        boolean hasError = false;
        try {
            DataFrame df;
            if (withoutLimit) {
                df = hContext.sql(sql);
            } else {
                if (sql.toLowerCase().contains("limit")) {
                    df = hContext.sql(sql);
                } else {
                    df = hContext.sql(sql + " limit 100");
                    log.info("*****************************" + sql + " limit 100");
                }
            }
            if (DBtype.trim().equalsIgnoreCase("hive")) {
                log.info("used hivedao to save is starting....");

                HiveDao sparkDao = (HiveDao) SparkDaoFactory.createDao(DBType.HIVE);
                log.info("===================================table name = " + tablename);
                sparkDao.save(hContext, df, tablename, format, mode);

                log.info("used hivedao to save is finished .....");
            }
            // mysqlDao
            else if (DBtype.trim().equalsIgnoreCase("mysql")) {
                log.info("used mysqldao to save is starting.....");

                MysqlDao sparkDao = (MysqlDao) SparkDaoFactory.createDao(DBType.MYSQL);
                log.info("===================================table name = " + tablename);
                sparkDao.save(hContext, df, tablename, mode);
                log.info("used mysqldao to save is finished.....");
            }
            // oracleDao
            else if (DBtype.trim().equalsIgnoreCase("oracle")) {
                log.info("used oracledao to save is starting.....");

                OracleDAO sparkDao = (OracleDAO) SparkDaoFactory.createDao(DBType.ORACLE);
                log.info("===================================table name = " + tablename);
                sparkDao.save(hContext, df, tablename, mode);

                log.info("used oracledao to save is finished.....");
            }
            // sdbDao
            else if (DBtype.trim().equalsIgnoreCase("sdb")) {
                log.info("used sdbdao to save is starting.....");

                SDBDao sparkDao = (SDBDao) SparkDaoFactory.createDao(DBType.SDB);
                log.info("===================================table name = " + tablename);
                sparkDao.save(hContext, df, tablename);

                log.info("used sdbdao to save is finished.....");
            }
            // ESDao
            else if (DBtype.trim().equalsIgnoreCase("es")) {
                log.info("used esdao to save is starting.....");

                ESDao sparkDao = (ESDao) SparkDaoFactory.createDao(DBType.ES);
                log.info("===================================table name = " + tablename);
                sparkDao.save(hContext, df, tablename);

                log.info("used esdao to save is finished.....");
            }
            // others
            else {
                log.info("the database used to save is not support now!");
            }
        } catch (Exception e) {
            e.printStackTrace();
            hasError = true;
        }
        if (hasError) {
            return 1;
        } else {
            return 0;
        }
    }

    public int run(HiveContext hContext, String sql, String DBtype, String tablename) {
        return run(hContext, sql, DBtype, tablename, false);
    }

    public int run(HiveContext hContext, String[] sql, String DBType, String[] tableNames, boolean withoutLimit) {
        boolean hasError = false;
        int count = sql.length - 1;
        try {
            if (sql.length != tableNames.length) {
                hasError = true;
                log.info("*****************************Error: sql.length() does not equal to tableNames.length()");
            } else {
                for (int i = 0; i < count; i++) {
                    DataFrame dfTmp;
                    dfTmp = hContext.sql(sql[i]);
                    if (!tableNames[i].trim().equalsIgnoreCase("") && tableNames != null) {
                        dfTmp.registerTempTable(tableNames[i]);
                        log.info("*****************************sql[" + i + "]:" + sql[i]
                                + ", register as table:" + tableNames[i]);
                    }
                }
                /*DataFrame[] dfTmp = new DataFrame[count];
                for (int i = 0; i < count; i++) {
                    dfTmp[i] = hContext.sql(sql[i]);
                    dfTmp[i].registerTempTable(tableNames[i]);
                    log.info("*****************************sql[" + i + "]:" + sql[i]
                            + ", register as table:" + tableNames[i]);
                }*/
                DataFrame df;
                if (withoutLimit) {
                    df = hContext.sql(sql[count]);
                    log.info("**************sql without limit = " + sql[count]);
                } else {
                    if (sql[count].toLowerCase().contains("limit")) {
                        df = hContext.sql(sql[count]);
                    } else {
                        df = hContext.sql(sql[count] + " limit 100");
                        log.info("*****************************" + sql[count] + " limit 100");
                    }
                }
                if (DBType.trim().equalsIgnoreCase("hive")) {
                    log.info("used hivedao to save is starting....");

                    HiveDao sparkDao = (HiveDao) SparkDaoFactory.createDao(Constant.DBType.HIVE);
                    log.info("===================================table name = " + tableNames[count]);
                    sparkDao.save(hContext, df, tableNames[count], format, mode);

                    log.info("used hivedao to save is finished .....");
                }
                // mysqlDao
                else if (DBType.trim().equalsIgnoreCase("mysql")) {
                    log.info("used mysqldao to save is starting.....");

                    MysqlDao sparkDao = (MysqlDao) SparkDaoFactory.createDao(Constant.DBType.MYSQL);
                    log.info("===================================table name = " + tableNames[count]);
                    sparkDao.save(hContext, df, tableNames[count], mode);
                    log.info("used mysqldao to save is finished.....");
                }
                // oracleDao
                else if (DBType.trim().equalsIgnoreCase("oracle")) {
                    log.info("used oracledao to save is starting.....");

                    OracleDAO sparkDao = (OracleDAO) SparkDaoFactory.createDao(Constant.DBType.ORACLE);
                    log.info("===================================table name = " + tableNames[count]);
                    sparkDao.save(hContext, df, tableNames[count], mode);

                    log.info("used oracledao to save is finished.....");
                }
                // sdbDao
                else if (DBType.trim().equalsIgnoreCase("sdb")) {
                    log.info("used sdbdao to save is starting.....");

                    SDBDao sparkDao = (SDBDao) SparkDaoFactory.createDao(Constant.DBType.SDB);
                    log.info("===================================table name = " + tableNames[count]);
                    sparkDao.save(hContext, df, tableNames[count]);

                    log.info("used sdbdao to save is finished.....");
                }
                // ESDao
                else if (DBType.trim().equalsIgnoreCase("es")) {
                    log.info("used esdao to save is starting.....");

                    ESDao sparkDao = (ESDao) SparkDaoFactory.createDao(Constant.DBType.ES);
                    log.info("===================================table name = " + tableNames[count]);
                    sparkDao.save(hContext, df, tableNames[count]);

                    log.info("used esdao to save is finished.....");
                }
                // others
                else {
                    log.info("the database used to save is not support now!");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            hasError = true;
        }
        if (hasError) {
            return 1;
        } else {
            return 0;
        }
    }

    public int run(HiveContext hContext, String[] sql, String DBType, String[] tableNames) {
        return run(hContext, sql, DBType, tableNames, false);
    }

    public int run(HiveContext hContext, ArrayList<String> sqlList, ArrayList<String> modeList,
                   ArrayList<String> dbTypeList, ArrayList<String> tableNameList, SaveLogUtils savelog, String dbName) {
        return run(hContext, sqlList, modeList, dbTypeList, tableNameList, savelog, dbName, "");
    }

    public int run(HiveContext hContext, ArrayList<String> sqlList, ArrayList<String> modeList,
                   ArrayList<String> dbTypeList, ArrayList<String> tableNameList, SaveLogUtils savelog, String dbName, String UUID) {
        boolean hasError = false;
        int count = sqlList.size();
        try {
            if (!(sqlList.size() == modeList.size() && modeList.size() == dbTypeList.size() && dbTypeList.size() == tableNameList.size())) {
                hasError = true;
                log.info("*****************************Error: all arrayLists's sizes are not equal...");
                savelog.write("process-sqls", "error", "Error: all arrayLists's sizes are not equal...");
            } else {
                hContext.sql("use " + dbName);
                log.info("***************** use " + dbName);
                for (int i = 0; i < count; i++) {
                    log.info("sqlList.get(" + i + ")=" + sqlList.get(i)
                            + ", modeList.get(" + i + ")=" + modeList.get(i)
                            + ", dbTypeList.get(" + i + ")=" + dbTypeList.get(i)
                            + ", tableNameList.get(" + i + ")=" + tableNameList.get(i));
                    String sql = sqlList.get(i);
                    if (sql.endsWith(";")) {
                        sql = sql.substring(0, sql.lastIndexOf(";"));
                    }
                    log.info("####sql[" + i + "] : " + sql);
                    DataFrame dfTmp = hContext.sql(sql);
                    if (modeList.get(i).equalsIgnoreCase("0")) { //0 表示临时表
                        if (!tableNameList.get(i).trim().equalsIgnoreCase("") && tableNameList.get(i) != null) {
                            dfTmp.registerTempTable(tableNameList.get(i));
                            log.info("*****************************sql[" + i + "]:" + sqlList.get(i)
                                    + ", register as table:" + tableNameList.get(i));
                            savelog.write("hsql[" + i + "]", "success", "The " + i + "-th table is processed...");
                        } else {
                            log.info("#######Warning: The " + i + "-th temporary table's is null...");
                            savelog.write("hsql[" + i + "]", "warning", "The " + i + "-th temporary table's is null...");
                        }
                    } else { //物理表
                        if (!tableNameList.get(i).trim().equalsIgnoreCase("") && tableNameList.get(i) != null
                                && !dbTypeList.get(i).trim().equalsIgnoreCase("") && dbTypeList.get(i) != null) {
                            log.info("*****************************sql[" + i + "]:" + sqlList.get(i));
                            if(tableNameList.get(i).trim().equalsIgnoreCase("bus_warning_list")){//hive warning{
                                if(dfTmp.count()>0){
                                    DataFrame ndf = dfTmp.withColumn("config_uid", org.apache.spark.sql.functions.lit(UUID)).cache();
                                    DataFrame warnList = ndf.selectExpr("uuid","warn_msg","warn_topic","warn_time","current_val","config_uid","status","limit_val","timestamp").distinct();
                                    log.info("*************start to save query result to bus_warning_list for warnning");
                                    save(hContext, warnList, "hive", "bus_warning_list");
                                    log.info("************* "+warnList.count()+" results have been saved into bus_warning_list for warnning");
                                    DataFrame warnMsg = ndf.selectExpr("uuid","message").distinct();
                                    log.info("*************start to save warning message to bus_warning_message for warnning");
                                    save(hContext, warnMsg, "hive", "bus_warning_message");
                                    log.info("************* "+warnMsg.count()+" warning msgs have been saved into bus_warning_message for warnning");
                                }else{
                                    log.info("*************the query result of warning is empty, nothing will be saved...");
                                }
                            }else {
                                save(hContext, dfTmp, dbTypeList.get(i).trim(), tableNameList.get(i).trim());
                                savelog.write("hsql[" + i + "]", "success", "The " + i + "-th table is processed...");
                            }
                        } else {
                            log.info("#######Error: The " + i + "-th physical table's name is null or its dbType is null...");
                            savelog.write("hsql[" + i + "]", "Error", "The " + i + "-th table's name is null or its dbType is null...");
                        }
                    }
                }
                /*DataFrame[] dfTmp = new DataFrame[count];
                for (int i = 0; i < count; i++) {
                    dfTmp[i] = hContext.sql(sql[i]);
                    dfTmp[i].registerTempTable(tableNames[i]);
                    log.info("*****************************sql[" + i + "]:" + sql[i]
                            + ", register as table:" + tableNames[i]);
                }*/
            }
        } catch (Exception e) {
            e.printStackTrace();
            hasError = true;
        }
        if (hasError) {
            return 1;
        } else {
            return 0;
        }
    }


    public void save(HiveContext hContext, DataFrame df, String DBType, String tableName) throws Exception {
        if (DBType.trim().equalsIgnoreCase("hive")) {
            log.info("used hivedao to save is starting....");

            HiveDao sparkDao = (HiveDao) SparkDaoFactory.createDao(Constant.DBType.HIVE);
            log.info("===================================table name = " + tableName);
            sparkDao.save(hContext, df, tableName);

            log.info("used hivedao to save is finished .....");
        }
        // mysqlDao
        else if (DBType.trim().equalsIgnoreCase("mysql")) {
            log.info("used mysqldao to save is starting.....");

            MysqlDao sparkDao = (MysqlDao) SparkDaoFactory.createDao(Constant.DBType.MYSQL);
            log.info("===================================table name = " + tableName);
            sparkDao.save(hContext, df, tableName);
            log.info("used mysqldao to save is finished.....");
        }
        // oracleDao
        else if (DBType.trim().equalsIgnoreCase("oracle")) {
            log.info("used oracledao to save is starting.....");

            OracleDAO sparkDao = (OracleDAO) SparkDaoFactory.createDao(Constant.DBType.ORACLE);
            log.info("===================================table name = " + tableName);
            sparkDao.save(hContext, df, tableName);

            log.info("used oracledao to save is finished.....");
        }
        // sdbDao
        else if (DBType.trim().equalsIgnoreCase("sdb")) {
            log.info("used sdbdao to save is starting.....");

            SDBDao sparkDao = (SDBDao) SparkDaoFactory.createDao(Constant.DBType.SDB);
            log.info("===================================table name = " + tableName);
            sparkDao.save(hContext, df, tableName);

            log.info("used sdbdao to save is finished.....");
        }
        // ESDao
        else if (DBType.trim().equalsIgnoreCase("es")) {
            log.info("used esdao to save is starting.....");

            ESDao sparkDao = (ESDao) SparkDaoFactory.createDao(Constant.DBType.ES);
            log.info("===================================table name = " + tableName);
            sparkDao.save(hContext, df, tableName);

            log.info("used esdao to save is finished.....");
        }
        // others
        else {
            log.info("the database used to save is not support now!");
        }
    }

    public void saveToMysql(HiveContext hContext, DataFrame df, String tablename) {
        log.info("saveToMysql is starting.....");

        MysqlDao sparkDao = (MysqlDao) SparkDaoFactory.createDao(DBType.MYSQL);
        log.info("===================================table name = " + tablename);
        sparkDao.save(hContext, df, tablename);
        log.info("saveToMysql is finished.....");
    }

}
