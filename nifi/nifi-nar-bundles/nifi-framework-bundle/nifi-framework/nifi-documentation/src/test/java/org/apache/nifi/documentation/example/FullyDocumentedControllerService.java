/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.documentation.example;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.util.StandardValidators;

@CapabilityDescription("A documented controller service that can help you do things")
@Tags({ "one", "two", "three" })
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
