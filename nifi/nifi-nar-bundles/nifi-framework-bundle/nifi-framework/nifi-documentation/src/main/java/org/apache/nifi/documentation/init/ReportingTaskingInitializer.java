package org.apache.nifi.documentation.init;

import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.documentation.ConfigurableComponentInitializer;
import org.apache.nifi.documentation.mock.MockReportingInitializationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingTask;

public class ReportingTaskingInitializer implements ConfigurableComponentInitializer {

	@Override
	public void initialize(ConfigurableComponent component) throws InitializationException {
		ReportingTask reportingTask = (ReportingTask) component;
		reportingTask.initialize(new MockReportingInitializationContext());
	}
}
