package com.cbtest.largescaletest.iteminfo;

import java.util.Date;

public class CallInfo {
	public String callToken;
	public String srcPhone;
	public Date timeCalled;
	public String contactName;
	
	public CallInfo(String callToken, String srcPhone, Date timeCalled) {
		this.callToken = callToken;
		this.srcPhone = srcPhone;
		this.timeCalled = timeCalled;
	}

	@Override
	public String toString() {
		return "CallInfo [callToken=" + callToken + ", srcPhone=" + srcPhone
				+ ", timeCalled=" + timeCalled + ", contactName=" + contactName
				+ "]";
	}

}
