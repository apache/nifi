package org.apache.nifi.documentation;

import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.reporting.InitializationException;

public interface ConfigurableComponentInitializer {
	void initialize(ConfigurableComponent component) throws InitializationException;
}
