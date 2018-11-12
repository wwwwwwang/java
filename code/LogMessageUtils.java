package com.datageek.bdr.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.text.SimpleDateFormat;
import java.util.Date;

@JsonPropertyOrder({"id", "type", "uuid", "ip", "time_stamp", "stage", "status", "date", "message"})
@JsonIgnoreProperties(ignoreUnknown = true) // To ignore any unknown properties in JSON input without exception
@JsonTypeName("content")
public class LogMessageUtils {
    private String id="";
    private String type="";
    private String ip="";
    private String uuid="";
    private Long time_stamp;
    private String stage="";
    private String status="";
    private String date;
    private String message;

    @JsonProperty("id")
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }

    @JsonProperty("ip")
    public String getIp() {
        return ip;
    }
    public void setIp(String ip) {
        this.ip = ip;
    }

    @JsonProperty("uuid")
    public String getUUid() {
        return uuid;
    }
    public void setUUid(String uuid) {
        this.uuid = uuid;
    }

    @JsonProperty("type")
    public String getType() {
        return type;
    }
    public void setType(String type) {
        this.type = type;
    }

    @JsonProperty("time_stamp")
    public Long getTimeStamp() {
        return time_stamp;
    }
    public void setTimeStamp(long time_stamp) {
        this.time_stamp = time_stamp;
    }

    @JsonProperty("stage")
    public String getStage() {
        return stage;
    }
    public void setStage(String stage) {
        this.stage = stage;
    }

    @JsonProperty("status")
    public String getStatus() {
        return status;
    }
    public void setStatus(String status) {
        this.status = status;
    }

    @JsonProperty("date")
    public String getDate() {
        return date;
    }
    public void setDate(String date) {
        this.date = date;
    }

    public String getMessage() {
        return message;
    }
    public void setMessage(String message) {
        this.message = message;
    }

    private static String longToDate(long time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  //MM/dd/yyyy HH:mm:ss
        Date dt = new Date(time);
        return sdf.format(dt);
    }

    public static String toString(String id, String type, String uuid, String ip, String stage, String status, String message) throws JsonProcessingException {
        //"id", "type","time_stamp", "stage", "status", "date", "message"
        LogMessageUtils log = new LogMessageUtils();
        long nowTime = System.currentTimeMillis();
        log.setId(id);
        log.setType(type);
        log.setUUid(uuid);
        log.setIp(ip);
        log.setStage(stage);
        log.setStatus(status);
        log.setTimeStamp(nowTime);
        log.setDate(longToDate(nowTime));
        log.setMessage(message);
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(log);
    }

    public static String toString(String id, String type, String stage, String status, String message) throws JsonProcessingException {
        //"id", "type","time_stamp", "stage", "status", "date", "message"
        LogMessageUtils log = new LogMessageUtils();
        long nowTime = System.currentTimeMillis();
        log.setId(id);
        log.setType(type);
        log.setStage(stage);
        log.setStatus(status);
        log.setTimeStamp(nowTime);
        log.setDate(longToDate(nowTime));
        log.setMessage(message);
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(log);
    }

    public static void main(String[] args) throws JsonProcessingException {
        String id = "1";
        String type = "cisco";
        String stage = "start";
        String status = "success";
        String message = "{\"key\":\"hello, I am test...\"}";

        String jsonStr = toString(id, type, stage, status, message);

        System.out.println(jsonStr);
    }
}
