package com.datageek.bdr.util;

import com.datageek.jdbcDao.MysqlJDBCDao;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;

public class SaveLogUtils implements Serializable{
    private static Logger log = Logger.getLogger(SaveLogUtils.class);
    private static Producer<String, String> producer;
    private String topic = "task_status_log";
    private String id = "";
    private String type = "";
    private String uuid = "";
    private String ip = "";

    /**
     * @param brokersList
     * @param id          the unique id of this application
     * @param type        the application type, such as cisco, etl_gxp, etc.
     */
    public SaveLogUtils(String topic, String brokersList, String id, String type) {
        if (!topic.equals("task_status_log")) {
            log.info(topic + " is not equal to task_status_log, using task_status_log replaced");
        }
        this.id = id;
        this.type = type;
        this.uuid = UUID.randomUUID().toString();
        String address;
        try {
            address = InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            e.printStackTrace();
            address = "Unknown";
        }
        this.ip = address;
        init(brokersList);
    }

    public SaveLogUtils(String brokersList, String id, String type) {
        this.id = id;
        this.type = type;
        this.uuid = UUID.randomUUID().toString();
        String address;
        try {
            address = InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            e.printStackTrace();
            address = "Unknown";
        }
        this.ip = address;
        init(brokersList);
    }

    public SaveLogUtils(String id, String type) {
        this.id = id;
        this.type = type;
        this.uuid = UUID.randomUUID().toString();
        String address;
        try {
            address = InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            e.printStackTrace();
            address = "Unknown";
        }
        this.ip = address;
        String brokersList = "";
        try {
            MysqlJDBCDao myjdbc = new MysqlJDBCDao();
            brokersList = myjdbc.getBrokersInDictionary();
            log.info("++++++++++++brokerlist = " + brokersList);
            myjdbc.closeConn();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        init(brokersList);
    }

    private void init(String brokersList) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokersList);
        //"172.31.18.12:9092,172.31.18.13:9092,172.31.18.14:9092"
        //props.put("acks", "all");
        //props.put("retries", 0);
        //props.put("batch.size", 16384);
        //props.put("linger.ms", 1);
        //props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer(props);
        log.info("producer has been initialized!");
    }

    /*private String makeString(String type, String id, String message) {
        String pid = "";
        if (!id.trim().equalsIgnoreCase("") && id != null) {
            pid = id;
        }
        long nowTime = System.currentTimeMillis();
        return "{\"id\":\"" + pid + "\",\"type\":\"" + type
                + "\",\"time_stamp\":\"" + nowTime
                + "\",\"date\":\"" + longToDate(nowTime)
                + "\",\"message\":\"" + message + "\"}";
    }*/

    private String makeString(String id, String type, String uuid, String ip, String stage, String status, String message)
            throws JsonProcessingException {
        return LogMessageUtils.toString(id, type, uuid, ip, stage, status, message);
    }

    /**
     * @param stage  the stage of the application, such as start, read, write, hive save, end, etc.
     * @param status the result of corresponding stage, such as success, fail, exception, etc.
     * @param raw    the raw message you want to save
     */
    public void write(String stage, String status, String raw) {
        String message;
        try {
            message = makeString(id, type, uuid, ip, stage, status, raw);
            log.info("The message is ready!\nmessage = " + message);
            producer.send(new ProducerRecord<>(topic, message));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        producer.close();
    }

    public static void main(String[] args) {
        String brokersList = "172.31.18.12:9092,172.31.18.13:9092,172.31.18.14:9092";
        String type = "test";
        String id = "";
        SaveLogUtils savelog = new SaveLogUtils("whsh", brokersList, id, type);
        String stage = "START";
        String status = "success";
        String message = "Just a test ......";
        savelog.write(stage, status, message);
        savelog.close();
        log.info("The message has been send! ");
    }
}
