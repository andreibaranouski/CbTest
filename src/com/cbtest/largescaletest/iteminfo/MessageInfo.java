package com.cbtest.largescaletest.iteminfo;

import java.util.Date;

public class MessageInfo {
	public String msgToken;
	public String srcPhone;
	public Date timeSent;
	public String text;
	
	public MessageInfo(String msgToken, String srcPhone, Date timeSent, String data) {
		this.msgToken = msgToken;
		this.srcPhone = srcPhone;
		this.timeSent = timeSent;
		this.text = data;
	}

	@Override
	public String toString() {
		return "MessageInfo [msgToken=" + msgToken + ", srcPhone=" + srcPhone
				+ ", timeSent=" + timeSent + ", text=" + text + "]";
	}
}
