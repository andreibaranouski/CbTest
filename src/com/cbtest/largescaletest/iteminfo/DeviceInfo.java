package com.cbtest.largescaletest.iteminfo;

public class DeviceInfo {
	public String PhoneNum;
	public String Capabilities;
	public String DeviceKey;
	public String PushToken;
	public String version;	
	public int cid;
	public int system;
	public int gennum;
	
	public DeviceInfo(String phoneNum, String capabilities, String deviceKey,
			String pushToken, String version, int cid, int system, int gennum) {
		super();
		PhoneNum = phoneNum;
		Capabilities = capabilities;
		DeviceKey = deviceKey;
		PushToken = pushToken;
		this.version = version;
		this.cid = cid;
		this.system = system;
		this.gennum = gennum;
	}
	@Override
	public String toString() {
		return "DeviceInfo [PhoneNum=" + PhoneNum + ", Capabilities="
				+ Capabilities + ", DeviceKey=" + DeviceKey + ", PushToken="
				+ PushToken + ", version=" + version + ", cid=" + cid
				+ ", system=" + system + ", gennum=" + gennum + "]";
	}
}
