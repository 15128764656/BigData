package com.bigdata.activemq;

import java.io.Serializable;

public class U implements Serializable{

    private static final long serialVersionUID = 2614847347889166013L;
	
	private int code=1;
	
	private String text="状态";
	
	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}
	//zhushi
	@Override
	public String toString() {
		return "U [code=" + code + ", text=" + text + "]";
	}
}
