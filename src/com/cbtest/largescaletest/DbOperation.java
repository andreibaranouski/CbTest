package com.cbtest.largescaletest;

import java.util.Date;

public class DbOperation {
	public DbOperationType dbOpType;
	public String phone;
	public String UDID;
	public Date time;
	public String srcPhone;
	public String text;

	public DbOperation(DbOperationType dbOpType, String phone) {
		this(dbOpType, phone, null, new Date(), null, null);
	}

	public DbOperation(DbOperationType dbOpType, String phone, String uDID,
			Date time, String srcPhone, String text) {
		super();
		this.dbOpType = dbOpType;
		this.phone = phone;
		UDID = uDID;
		this.time = time;
		this.srcPhone = srcPhone;
		this.text = text;
	}

	@Override
	public String toString() {
		return "DbOperation [dbOpType=" + dbOpType + ", phone=" + phone
				+ ", UDID=" + UDID + ", time=" + time + ", srcPhone="
				+ srcPhone + ", text=" + text + "]";
	}
}
