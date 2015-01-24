package org.apache.nifi.documentation.mock;

import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.controller.ControllerServiceLookup;

public class MockControllerServiceInitializationContext implements ControllerServiceInitializationContext{

	@Override
	public String getIdentifier() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ControllerServiceLookup getControllerServiceLookup() {
		return new MockControllerServiceLookup();
	}

}
