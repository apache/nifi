package org.apache.nifi.documentation.init;

import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.documentation.ConfigurableComponentInitializer;
import org.apache.nifi.documentation.mock.MockProcessorInitializationContext;
import org.apache.nifi.processor.Processor;

public class ProcessorInitializer implements ConfigurableComponentInitializer {

	@Override
	public void initialize(ConfigurableComponent component) {
		Processor processor = (Processor) component;
		processor.initialize(new MockProcessorInitializationContext());
	}

}
