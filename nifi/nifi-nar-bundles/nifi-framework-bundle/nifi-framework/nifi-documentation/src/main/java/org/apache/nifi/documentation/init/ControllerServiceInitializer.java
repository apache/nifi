package org.apache.nifi.documentation.init;

import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.documentation.ConfigurableComponentInitializer;
import org.apache.nifi.documentation.mock.MockControllerServiceInitializationContext;
import org.apache.nifi.reporting.InitializationException;

public class ControllerServiceInitializer implements ConfigurableComponentInitializer {

	@Override
	public void initialize(ConfigurableComponent component) throws InitializationException {
		ControllerService controllerService = (ControllerService) component;
		controllerService.initialize(new MockControllerServiceInitializationContext());
	}

}
