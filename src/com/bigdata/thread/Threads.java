package com.bigdata.thread;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;

public class Threads extends Thread{

    StuDao sd;
    public Threads(StuDao sd){
    	this.sd=sd;
    }
	
	@Override
	public void run() {
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		for(int j=0;j<20;j++){
			sd.k++;
			//getCon(j+1);
			sd.getObjById("1");
		}
	}
	
	public Connection getCon(int n){
		//加载驱动
		Connection con=null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con=DriverManager.getConnection("jdbc:oracle:thin:localhost:1521:orcl", "test", "test");
			System.out.println("线程"+Thread.currentThread().getName()+"执行第"+(n)+",次con连接对象=="+con);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return con;
	}
	public static void main(String[] args) {
		//开启线程
		StuDao sd=new StuDao();
		for(int i=1;i<=10;i++){
			Threads ts=new Threads(sd);
			ts.setName("线程"+i);
			ts.start();
		}
		while(Thread.activeCount()>1){
			Thread.yield();
		}
		//System.out.println(sd.k);
		try {
			System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
