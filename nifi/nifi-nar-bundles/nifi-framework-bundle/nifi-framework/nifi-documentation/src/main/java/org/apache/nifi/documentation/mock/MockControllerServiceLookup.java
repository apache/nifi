package org.apache.nifi.documentation.mock;

import java.util.Set;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;

public class MockControllerServiceLookup implements ControllerServiceLookup {

	@Override
	public ControllerService getControllerService(String serviceIdentifier) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isControllerServiceEnabled(String serviceIdentifier) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isControllerServiceEnabled(ControllerService service) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Set<String> getControllerServiceIdentifiers(Class<? extends ControllerService> serviceType)
			throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

}
