package org.apache.nifi.bootstrap;

public class ShutdownHook extends Thread {
	private final Process nifiProcess;
	
	public ShutdownHook(final Process nifiProcess) {
		this.nifiProcess = nifiProcess;
	}
	
	@Override
	public void run() {
		nifiProcess.destroy();
	}
}
