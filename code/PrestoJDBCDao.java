package com.datageek.bdr.dao;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import com.datageek.jdbcDao.MysqlJDBCDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datageek.bdr.util.PropertiesUtils;

public class PrestoJDBCDao {
	public static boolean isUsingConfigFile = false;
	static Logger log = LoggerFactory.getLogger(PrestoJDBCDao.class);
	public static String driverName  = "";
	public static String dbUrl = ""; 
	public static String dbUser  = "";
	public static String dbPassword  = "";
	public String sql = "";
	public static Connection conn = null;
	public static Statement stmt = null;
	static{
		if (true == isUsingConfigFile) {
			try {
				PropertiesUtils.loadPropertiesByClassPath("conn.properties");
			} catch (IOException e) {
				e.printStackTrace();
			}
			driverName = PropertiesUtils.getProperty("db.prestojdbc.driverName");
			dbUrl = PropertiesUtils.getProperty("db.prestojdbc.url");
			dbUser = PropertiesUtils.getProperty("db.prestojdbc.user");
			dbPassword = PropertiesUtils.getProperty("db.prestojdbc.password");
		} else {
			MysqlJDBCDao mysqljdbcDao = new MysqlJDBCDao();
			try {
				HashMap<String, String> prestojdbc = mysqljdbcDao.getPrestoInfo();
				log.info("HashMap<String, String> prestojdbc.size=" + prestojdbc.size());
				driverName = prestojdbc.get("db.prestojdbc.driverName");
				dbUrl = prestojdbc.get("db.prestojdbc.url");
				dbUser = prestojdbc.get("db.prestojdbc.user");
				dbPassword = prestojdbc.get("db.prestojdbc.password");
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		log.info("db.prestojdbc.driverName:" + driverName);
		log.info("db.prestojdbc.url:" + dbUrl);
		log.info("db.prestojdbc.user:" + dbUser);

	}

	public static void closeConn() throws SQLException {
		if(stmt != null)
			stmt.close();
		if(conn != null)
			conn.close();
    }  
  
	public static int count(ResultSet rest) throws SQLException{
		int rowCount = 0; //得到当前行号，也就是记录数
		while(rest.next()){
			rowCount++;
		}
		rest.beforeFirst();
		return rowCount;
	}
	
	public static ResultSet query(String sql) throws SQLException, ClassNotFoundException {
		try {
			Class.forName(driverName);
			conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
			//stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			stmt = conn.createStatement();
		} catch (ClassNotFoundException | SQLException e1) {
			// TODO Auto-generated catch block
		}
		//long begin = System.currentTimeMillis();
		ResultSet res = stmt.executeQuery(sql);
		//long end = System.currentTimeMillis();
		//System.out.println("query ends, using time is " + (end - begin) + " ms ");
		/*while(rs.next()){
			System.out.println("res is "+rs.getInt(1));
		}*/
		//System.out.println("query the sql is finished, get "+count(res)+" results!");
		closeConn();
		return res;
	}

	public static ResultSet query(String sql, String DBName)
			throws SQLException, ClassNotFoundException {
		dbUrl = dbUrl.substring(0,dbUrl.lastIndexOf('/')+1) + DBName;
		log.info("new db.prestojdbc.url:" + dbUrl);
		return query(sql);
	}

}