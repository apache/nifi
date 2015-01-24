package org.apache.nifi.documentation.mock;

import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessorInitializationContext;

public class MockProcessorInitializationContext implements ProcessorInitializationContext {

	@Override
	public String getIdentifier() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ProcessorLog getLogger() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ControllerServiceLookup getControllerServiceLookup() {
		return new MockControllerServiceLookup();
	}

}
