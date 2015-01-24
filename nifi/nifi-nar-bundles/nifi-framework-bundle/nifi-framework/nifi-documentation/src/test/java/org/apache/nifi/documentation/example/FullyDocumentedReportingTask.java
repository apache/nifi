package org.apache.nifi.documentation.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.annotation.CapabilityDescription;
import org.apache.nifi.processor.annotation.Tags;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;

@CapabilityDescription("A helper reporting task to do...")
@Tags({ "first", "second", "third" })
public class FullyDocumentedReportingTask extends AbstractReportingTask {

	public static final PropertyDescriptor SHOW_DELTAS = new PropertyDescriptor.Builder()
			.name("Show Deltas")
			.description(
					"Specifies whether or not to show the difference in values between the current status and the previous status")
			.required(true).allowableValues("true", "false").defaultValue("true").build();

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		final List<PropertyDescriptor> descriptors = new ArrayList<>();
		descriptors.add(SHOW_DELTAS);
		return descriptors;
	}

	@Override
	public void onTrigger(ReportingContext context) {
		// TODO Auto-generated method stub

	}
}
