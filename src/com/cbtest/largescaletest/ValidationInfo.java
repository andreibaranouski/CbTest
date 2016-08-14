package com.cbtest.largescaletest;

public class ValidationInfo {
	boolean isValid = true;
	String info = "";
	
	public ValidationInfo(boolean isValid, String info) {
		super();
		this.isValid = isValid;
		this.info = info;
	}

	@Override
	public String toString() {
		return "ValidationInfo [isValid=" + isValid + ", info=" + info + "]";
	}	
}
