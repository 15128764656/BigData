package com.bigdata.thread;

import java.math.BigDecimal;
import java.sql.Date;

public class Student {
	
	private String snum;
	private String sname;
	private int sage;
	private Date indate;
	private BigDecimal salary;
	private String sex;
	private Grade grade;
	
	
	public String getSnum() {
		return snum;
	}
	public void setSnum(String snum) {
		this.snum = snum;
	}
	public String getSname() {
		return sname;
	}
	public void setSname(String sname) {
		this.sname = sname;
	}
	public int getSage() {
		return sage;
	}
	public void setSage(int sage) {
		this.sage = sage;
	}
	public Date getIndate() {
		return indate;
	}
	public void setIndate(Date indate) {
		this.indate = indate;
	}
	public BigDecimal getSalary() {
		return salary;
	}
	public void setSalary(BigDecimal salary) {
		this.salary = salary;
	}
	public String getSex() {
		return sex;
	}
	public void setSex(String sex) {
		this.sex = sex;
	}
	public Grade getGrade() {
		return grade;
	}
	public void setGrade(Grade grade) {
		this.grade = grade;
	}
	@Override
	public String toString() {
		return "snum=" + snum + ", sname=" + sname + ", sage=" + sage
				+ ", indate=" + indate + ", salary=" + salary + ", sex=" + sex
				+ ", grade=" + grade;
	}
}
