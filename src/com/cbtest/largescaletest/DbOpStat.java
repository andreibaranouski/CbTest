package com.cbtest.largescaletest;

public class DbOpStat {
	DbOperationType dbOpType;
	public long numOps;
	public long sumOpsTime;
	public long maxOpTime;
	public long minOpTime;
	
	public DbOpStat(DbOperationType dbOpType) {
		this.dbOpType = dbOpType;
		reset();
	}
	
	public void reset() {
		numOps = 0;
		sumOpsTime = 0;
		maxOpTime = 0;
		minOpTime = Integer.MAX_VALUE;
	}
	
	public void addOp(long processingTime) {
		numOps++;
		sumOpsTime += processingTime;
		if (processingTime > maxOpTime)
			maxOpTime = processingTime;
		if (processingTime < minOpTime)
			minOpTime = processingTime;
	}
}
