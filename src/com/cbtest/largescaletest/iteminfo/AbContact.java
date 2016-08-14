package com.cbtest.largescaletest.iteminfo;

public class AbContact {
	public String name;
	public String orgNum;
	
	public AbContact(String name, String orgNum) {
		this.name = name;
		this.orgNum = orgNum;
	}
	
	@Override
	public String toString() {
		return "contact[" + name + "," + orgNum + "]";
	}
}
