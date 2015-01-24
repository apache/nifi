package org.apache.nifi.documentation.example;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.annotation.CapabilityDescription;
import org.apache.nifi.processor.annotation.Tags;
import org.apache.nifi.processor.util.StandardValidators;

@CapabilityDescription("A documented controller service that can help you do things")
@Tags({"one", "two", "three"})
public class FullyDocumentedControllerService extends AbstractControllerService {
	public static final PropertyDescriptor KEYSTORE = new PropertyDescriptor.Builder().name("Keystore Filename")
			.description("The fully-qualified filename of the Keystore").defaultValue(null)
			.addValidator(StandardValidators.FILE_EXISTS_VALIDATOR).sensitive(false).build();
	public static final PropertyDescriptor KEYSTORE_TYPE = new PropertyDescriptor.Builder().name("Keystore Type")
			.description("The Type of the Keystore").allowableValues("JKS", "PKCS12")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).defaultValue("JKS").sensitive(false).build();
	public static final PropertyDescriptor KEYSTORE_PASSWORD = new PropertyDescriptor.Builder()
			.name("Keystore Password").defaultValue(null).description("The password for the Keystore")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).sensitive(true).build();

	private static final List<PropertyDescriptor> properties;

	static {
		List<PropertyDescriptor> props = new ArrayList<>();
		props.add(KEYSTORE);
		props.add(KEYSTORE_PASSWORD);
		props.add(KEYSTORE_TYPE);
		properties = Collections.unmodifiableList(props);
	}

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return properties;
	}

}
