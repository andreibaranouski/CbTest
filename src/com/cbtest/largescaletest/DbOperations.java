package com.cbtest.largescaletest;

import java.util.ArrayList;

public class DbOperations {
	ArrayList<DbOperation> dbOps = new ArrayList<DbOperation>();
	
	public void addDbOp(DbOperation dbOp) {
		dbOps.add(dbOp);
	}
	
	@Override
	public String toString() {
		return dbOps.toString();
	}
}
