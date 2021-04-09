package com.bigdata.thread;

public class Grade {
	
	private String gid;
	
	private String gname;
	
	public Grade() {
	}
	
	public Grade(String gid, String gname) {
		super();
		this.gid = gid;
		this.gname = gname;
	}
	public String getGid() {
		return gid;
	}
	public void setGid(String gid) {
		this.gid = gid;
	}
	public String getGname() {
		return gname;
	}
	public void setGname(String gname) {
		this.gname = gname;
	}
	@Override
	public String toString() {
		return "Grade [gid=" + gid + ", gname=" + gname + "]";
	}
	
	

}
