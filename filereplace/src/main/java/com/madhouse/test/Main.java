package com.madhouse.test;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Main {

    private static Connection conn = null;
    private static Statement stmt = null;
    private static HashMap<String, String> cache = new HashMap<String, String>();

    public static void MysqlJDBC(String url, String user, String passwd) {
        String driverName = "com.mysql.jdbc.Driver";
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, user, passwd);
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void closeConn() {
        try {
            if (stmt != null)
                stmt.close();
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static String getNameByCode(String code) {
        String sql = "select name from md_audience_tag where code='" + code + "'";
        ResultSet res = null;
        String returnString = "";
        try {
            res = stmt.executeQuery(sql);

            if (res.next()) {
                returnString = res.getString("indexName").trim();
            }
            if (returnString.equalsIgnoreCase("")) {
                returnString = code;
            }
            res.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return returnString;
    }

    public static ArrayList<File> getFiles(String path) {
        //目标集合fileList
        ArrayList<File> fileList = new ArrayList<File>();
        File file = new File(path);
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File fileIndex : files) {
                //如果这个文件是目录，则进行递归搜索
                if (fileIndex.isDirectory()) {
                    getFiles(fileIndex.getPath());
                } else {//如果文件是普通文件，则将文件句柄放入集合中　　
                    fileList.add(fileIndex);
                }
            }
        }
        return fileList;
    }

    public static void prepare(String path) {
        File readFile = new File(path);
        //输入IO流声明
        InputStream in = null;
        InputStreamReader ir = null;
        BufferedReader br = null;
        cache.clear();
        try {
            in = new BufferedInputStream(new FileInputStream(readFile));
            ir = new InputStreamReader(in, "utf-8");
            br = new BufferedReader(ir);
            String line = "";

            while ((line = br.readLine()) != null) {
                //System.out.println(line);
                String[] words = line.split(",", 2);
                cache.put(words[0], words[1]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
                if (ir != null) {
                    ir.close();
                }
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
            }
        }
    }

    public static void process(String path) {
        File readFile = new File(path);
        //输入IO流声明
        InputStream in = null;
        InputStreamReader ir = null;
        BufferedReader br = null;
        BufferedWriter bw = null;

        try {
            in = new BufferedInputStream(new FileInputStream(readFile));
            ir = new InputStreamReader(in, "utf-8");
            br = new BufferedReader(ir);
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(path.replaceFirst("\\.log", "\\.csv"))),
                    "UTF-8"));
            String line = "";

            while ((line = br.readLine()) != null) {
                System.out.println(line);
                if (line.length() > 3) {
                    String resString = line.replace(":", ",");
                    String codesString = line.split(":")[1];
                    String[] codes = codesString.split(",");
                    for (String code : codes) {
                        //System.out.println(code);
                        String newName = cache.get(code.substring(3));
                        System.out.println(code.substring(3) + "------------>" + newName);
                        resString = resString.replaceFirst(code, newName);
                    /*if (cache.containsKey("code")) {
                        resString = resString.replaceFirst(code, cache.get(code));
                    } else {
                        String newName = getNameByCode(code.substring(3));
                        System.out.println("The code " + code + " will be changed to " + newName);
                        resString = resString.replaceFirst(code, newName);
                        cache.put(code, newName);
                    }*/
                    }
                    //resString.replaceFirst(":",",");
                    System.out.println("resString = " + resString);
                    bw.write(resString);
                    bw.newLine();
                }
            }

        } catch (Exception e) {

            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
                if (ir != null) {
                    ir.close();
                }
                if (in != null) {
                    in.close();
                }
                if (bw != null) {
                    bw.close();
                }
            } catch (Exception e2) {
            }
        }
    }

    public static void filter(String path) {
        File readFile = new File(path);
        //输入IO流声明
        InputStream in = null;
        InputStreamReader ir = null;
        BufferedReader br = null;
        BufferedWriter bw = null;

        try {
            in = new BufferedInputStream(new FileInputStream(readFile));
            ir = new InputStreamReader(in, "utf-8");
            br = new BufferedReader(ir);
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(path.replaceFirst("\\.log", "\\.csv"))),
                    "UTF-8"));
            String line = "";

            String tag = "011004001008011,011004001008010,017002007002";
            List<String> taglist = Arrays.asList(tag.split(","));

            int cnt = 0;

            while ((line = br.readLine()) != null) {
                //System.out.println(line);
                if (line.contains(",")) {
                    String key = line.split(":")[0];
                    String codesString = line.split(":")[1];
                    String[] codes = codesString.split(",");
                    List<String> tags = new ArrayList<String>();
                    for (String code : codes) {
                        tags.add(code.substring(3));
                    }
                    tags.retainAll(taglist);
                    if (tags.size() > 0) {
                        cnt++;
                        bw.write(key);
                        bw.newLine();
                        System.out.print(tags);
                        System.out.println("rightkey = " + key);
                    }

                }
            }
            System.out.println("there are " + cnt + " rightkey in the file:" + path);

        } catch (Exception e) {

            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
                if (ir != null) {
                    ir.close();
                }
                if (in != null) {
                    in.close();
                }
                if (bw != null) {
                    bw.close();
                }
            } catch (Exception e2) {
            }
        }
    }

    public static void portMap() {
        //String user = "wanghaishen";//SSH连接用户名
        //String password = "wanghaishen";//SSH连接密码
        String user = "yangxiaolei";//SSH连接用户名
        String password = "123456";//SSH连接密码
        String host = "114.80.90.98";//SSH服务器
        int port = 33000;//SSH访问端口
        try {
            JSch jsch = new JSch();
            Session session = jsch.getSession(user, host, port);
            session.setPassword(password);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();
            System.out.println(session.getServerVersion());
            int assinged_port = session.setPortForwardingL(3306, "localhost", 3306);
            System.out.println("localhost:" + assinged_port + " -> " + "localhost" + ":" + 3306);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
//        String url = "jdbc:mysql://10.10.9.119:3306/mahad";
//        String user = "readonly";
//        String passwd = "madhouse#(2017";
        String path = "C:\\Users\\Madhouse\\Desktop\\tag\\newtag";
        String filepath = "C:\\Users\\Madhouse\\Desktop\\tag\\md_audience_tag.txt";
        //portMap();
        //MysqlJDBC(url, user, passwd);
        /*prepare(filepath);
        System.out.println("cache.size="+cache.size());

        ArrayList<File> fileList = getFiles(path);

        for (File file : fileList) {
            System.out.println("=======================file path = "+file.getPath());
            process(file.getPath());
        }*/

        String travelPath = "C:\\Users\\Madhouse\\Desktop\\tag\\travellabel";

        ArrayList<File> fileList = getFiles(travelPath);

        for (File file : fileList) {
            System.out.println("=======================file path = " + file.getPath());
            filter(file.getPath());
        }

        //closeConn();
    }
}
