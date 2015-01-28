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
import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
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
