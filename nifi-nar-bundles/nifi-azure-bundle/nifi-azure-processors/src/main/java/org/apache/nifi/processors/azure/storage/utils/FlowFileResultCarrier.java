package org.apache.nifi.processors.azure.storage.utils;

import org.apache.nifi.flowfile.FlowFile;

public class FlowFileResultCarrier<T> {

	final private FlowFile flowFile;
	final private T result;
	final private Throwable exception;
	
	public FlowFileResultCarrier(FlowFile flowFile, T result) {
		this.flowFile = flowFile;
		this.result = result;
		this.exception = null;
	}
	
	public FlowFileResultCarrier(FlowFile flowFile, T result, Throwable exception) {
		this.flowFile = flowFile;
		this.result = result;
		this.exception = exception;
	}
	
	public FlowFile getFlowFile() {
		return flowFile;
	}

	public T getResult() {
		return result;
	}

	public Throwable getException() {
		return exception;
	}
	
}
