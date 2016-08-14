package com.cbtest.largescaletest;


public class ShutdownInterceptor extends Thread {
	private IApp app;

	public ShutdownInterceptor(IApp app) {
		this.app = app;
	}
	
	public void run() {
		app.shutDown();
	}
}