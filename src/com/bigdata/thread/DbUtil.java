package com.bigdata.thread;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 连接数据库的工具类
 * @author Administrator
 *
 */
public class DbUtil {
	static{
		System.err.println("我是静态代码块...");
	}
	
	public static void main(String[] args) {
		/*try {
			Class.forName("com.hxzy.jdbc.DbUtil");
		} catch (Exception e) {
			e.printStackTrace();
		}*/
		
		//获取连接
		getCon();
	}
	
	public static Connection getCon(){
		//加载驱动
		Connection con=null;
		try {
			/**
			 * 1、装载。将字节码读入内存，并产生一个与之对应的java.lang.Class类对象
               2、连接。这一步会验证字节码，为static变量分配内存，并赋默认值(0或null)
               3、初始化。为类的static变量赋初始值，假如有static int a = 1;这个将a赋值为1的操作就是这个时候做的。除此之外，还要调用类的static块
               
               if (defaultDriver == null) {
			        defaultDriver = (OracleDriver)new oracle.jdbc.OracleDriver();
			        DriverManager.registerDriver(defaultDriver);
			   } 
			 */
			Class.forName("oracle.jdbc.driver.OracleDriver");
			//连接指定数据库，返回一个连接对象
			/**
			 * jdbc 通过jdbc 技术连接数据库
			 * oracle 表示连接oracel
			 * thin 表示同廋客户端的方式连接
			 * localhost 数据库服务器ip地址,如果是连接本机，则用localhost 或者127.0.0.1
			 * 1521 oracel 数据库的端口
			 * orcl oracel的实例名称
			 */
			con=DriverManager.getConnection("jdbc:oracle:thin:localhost:1521:orcl", "test", "test");
			System.err.println("con连接对象=="+con);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return con;
	}
    
	//关闭资源
	public static void close(Statement ps,ResultSet rs,Connection con){
		try {
			if(rs!=null){
				rs.close();
			}
			if(ps!=null){
				ps.close();
			}
			if(con!=null){
				con.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
