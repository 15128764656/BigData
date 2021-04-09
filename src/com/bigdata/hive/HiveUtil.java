package com.bigdata.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.lang.StringUtils;

public class HiveUtil {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	private static String ip = "192.168.1.105";

	/**
	 * 创建连接对象
	 * @return
	 * root 为 hadoop的用户名与密码
	 * 连接前要先启动 hiveserver 命令： hive --service hiveserver2
	 * testhivedatabase 是自己创建的数据库名称，默认为default
	 */
	public static Connection getCon() {
		Connection con=null;
		try {
			Class.forName(driverName);
			con = DriverManager.getConnection("jdbc:hive2://" + ip+ ":10000/hivedatabase", "", "");
			// Statement stmt = con.createStatement();
			System.out.println("con连接对象==" + con);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return con;
	}
	
	/**
	 * 关闭资源
	 * @param con
	 */
    public static void close(Connection con,Statement st,ResultSet rs){
    	try {
    		if(rs!=null){
    			rs.close();
    		}
    		if(st!=null){
    			st.close();
    		}
    		if(con!=null){
        		con.close();
        	}
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    
	/**
	 * 创建数据库
	 * @param databasename
	 */
    public static void createDataBase(String databasename){
    	Connection con=null;
    	Statement ps=null;
    	try {
    		con=getCon();
        	String sql="create database if not exists "+databasename+" ";
        	ps = con.createStatement();
        	ps.execute(sql);
        	System.out.println("数据库"+databasename+"创建成功");
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			close(con,ps,null);
		}
    }
    /**
	 * 删除数据库
	 * @param databasename
	 */
    public static void dropDataBase(String databasename){
    	Connection con=null;
    	PreparedStatement ps=null;
    	try {
    		con=getCon();
        	String sql="drop database if exists "+databasename+" CASCADE";
        	ps = con.prepareStatement(sql);
        	boolean f=ps.execute();
        	System.out.println(f+"数据库"+databasename+"删除成功");
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			close(con,ps,null);
		}
    }
    
    /**
     * 创建表
     * 表在那个数据库下，看url连接用的database
     */
    public static void createTable(String tablename,String clonums,String fieldsterminatedby,String linesterminatedby){
    	Connection con=null;
    	PreparedStatement ps=null;
    	clonums=StringUtils.isEmpty(clonums)?"id int, value String":clonums;
    	fieldsterminatedby=StringUtils.isEmpty(fieldsterminatedby)?"'\t'":fieldsterminatedby;
    	linesterminatedby=StringUtils.isEmpty(linesterminatedby)?"'\n'":linesterminatedby;
    	try {
    		con=getCon();
        	String sql="CREATE TABLE IF NOT EXISTS "
			         +" "+tablename+" ("+clonums+")"
			         +" COMMENT '"+tablename+" details' "
			         +" ROW FORMAT DELIMITED"
			         +" FIELDS TERMINATED BY "+fieldsterminatedby+""
			         +" LINES TERMINATED BY "+linesterminatedby+""
			         +" STORED AS TEXTFILE";
        	ps = con.prepareStatement(sql);
        	ps.execute();
        	System.out.println("数据库表"+tablename+"创建成功");
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			close(con,ps,null);
		}
    }
    /**
     * 加载文件到 数据库表
     */
    public static void loadTableData(String tablename,String filpathname){
    	Connection con=null;
    	PreparedStatement ps=null;
    	filpathname=StringUtils.isEmpty(filpathname)?"'/logs/log.log'":filpathname;
    	try {
    		con=getCon();
        	String sql="LOAD DATA LOCAL INPATH "+filpathname+" " + "OVERWRITE INTO TABLE "+tablename+" ";
        	ps = con.prepareStatement(sql);
        	ps.execute();
        	System.out.println("数据库表"+tablename+"加载文件"+filpathname+"成功");
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			close(con,ps,null);
		}
    }
    
    /**
     * 删除表
     * 表在那个数据库下，看url连接用的database
     */
    public static void DropTable(String tablename){
    	Connection con=null;
    	PreparedStatement ps=null;
    	try {
    		con=getCon();
        	String sql="DROP TABLE IF EXISTS "+tablename+" ";
        	ps = con.prepareStatement(sql);
        	ps.execute();
        	System.out.println("数据库表"+tablename+"删除成功");
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			close(con,ps,null);
		}
    }
    
    /**
     * 查询
     * @param tablename
     * @param sql
     */
    public static void query(String tablename,String sql,String[] clos) {
    	Connection con=null;
    	PreparedStatement ps=null;
    	ResultSet rs=null;
    	sql=StringUtils.isEmpty(sql)?"select * from "+tablename+" ":sql;
    	StringBuffer bf=new StringBuffer("");
    	try {
    		con=getCon();
        	ps = con.prepareStatement(sql);
        	rs = ps.executeQuery();
        	while(rs.next()){
        		bf.append("\n");
        		for(int j=0;j<clos.length;j++){
        			Object obj=rs.getObject(clos[j]);
        			bf.append("\t"+obj);
        		} 
        		
        	}
        	System.out.println("数据库表"+tablename+"查询结果"+bf.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			close(con,ps,rs);
		}
    }
    
	public static void main(String[] args) {
		//1、创建连接对象
		//getCon();
		
		//2、创建数据库
		//createDataBase("mydb");
		
		//3、删除数据库
		//dropDataBase("mydb");
		
		//6、删除表
		//DropTable("mytable");
		
		//4、创建表
		//createTable("mytable","","'\t'","'\n'");
		
        //5、在表中插入数据
		//loadTableData("mytable","'/usr/java/mytable.txt'");
		
		//7、查询数据
		//query("qx","",new String[]{"rq","dz","wd"});
		
	}
}
