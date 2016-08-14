package com.cbtest.largescaletest;

public enum Table {
	Phones("PHN_", BucketName.UserInfo),
	Devices("DEV_", BucketName.UserInfo),
	RegNums("REG_", BucketName.AbRegNums),
	AB("AB_", BucketName.AbRegNums),
	RevAB("RAB_", BucketName.RevAB),
	Msgs("MSG_", BucketName.MsgsCalls),
	MsgsIdx("MSGS_", BucketName.MsgsCalls),
	DelMsgs("DMSG_", BucketName.MsgsCalls),
	DelMsgsIdx("DMSGS_", BucketName.MsgsCalls),
	Calls("CALL_", BucketName.MsgsCalls),
	CallsIdx("CALLS_", BucketName.MsgsCalls),
	DelCalls("DCALL_", BucketName.MsgsCalls),
	DelCallsIdx("DCALLS_", BucketName.MsgsCalls),
	;
	public String prefix;
	public BucketName bucket;
	
	private Table(String prefix, BucketName bucket) {
		this.prefix = prefix;
		this.bucket = bucket;
	}	
}
