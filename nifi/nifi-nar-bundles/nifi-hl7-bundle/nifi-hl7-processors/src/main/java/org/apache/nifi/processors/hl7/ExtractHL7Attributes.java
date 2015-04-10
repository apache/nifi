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
package org.apache.nifi.processors.hl7;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Composite;
import ca.uhn.hl7v2.model.Group;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.Primitive;
import ca.uhn.hl7v2.model.Segment;
import ca.uhn.hl7v2.model.Structure;
import ca.uhn.hl7v2.model.Type;
import ca.uhn.hl7v2.model.Varies;
import ca.uhn.hl7v2.parser.PipeParser;
import ca.uhn.hl7v2.validation.impl.ValidationContextFactory;


@SideEffectFree
@SupportsBatching
@Tags({"HL7", "health level 7", "healthcare", "extract", "attributes"})
@CapabilityDescription("Extracts information from an HL7 (Health Level 7) formatted FlowFile and adds the information as FlowFile Attributes. "
		+ "The attributes are named as <Segment Name> <dot> <Field Index>. If the segment is repeating, the naming will be "
		+ "<Segment Name> <underscore> <Segment Index> <dot> <Field Index>. For example, we may have an attribute named \"MHS.12\" with "
		+ "a value of \"2.1\" and an attribute named \"OBX_11.3\" with a value of \"93000^CPT4\".")
public class ExtractHL7Attributes extends AbstractProcessor {
	public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
		.name("Character Encoding")
		.description("The Character Encoding that is used to encode the HL7 data")
		.required(true)
		.expressionLanguageSupported(true)
		.addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
		.defaultValue("UTF-8")
		.build();

	public static final Relationship REL_SUCCESS = new Relationship.Builder()
		.name("success")
		.description("A FlowFile is routed to this relationship if it is properly parsed as HL7 and its attributes extracted")
		.build();
	
	public static final Relationship REL_FAILURE = new Relationship.Builder()
		.name("failure")
		.description("A FlowFile is routed to this relationship if it cannot be mapped to FlowFile Attributes. This would happen if the FlowFile does not contain valid HL7 data")
		.build();
	
	
	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		final List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(CHARACTER_SET);
		return properties;
	}
	
	@Override
	public Set<Relationship> getRelationships() {
		final Set<Relationship> relationships = new HashSet<>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		return relationships;
	}
	
	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if ( flowFile == null ) {
			return;
		}
		
		final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).evaluateAttributeExpressions(flowFile).getValue());
		
		final byte[] buffer = new byte[(int) flowFile.getSize()];
		session.read(flowFile, new InputStreamCallback() {
			@Override
			public void process(final InputStream in) throws IOException {
				StreamUtils.fillBuffer(in, buffer);
			}
		});

		@SuppressWarnings("resource")
		final HapiContext hapiContext = new DefaultHapiContext();
		hapiContext.setValidationContext(ValidationContextFactory.noValidation());
		
		final PipeParser parser = hapiContext.getPipeParser();
		final String hl7Text = new String(buffer, charset);
		final Message message;
		try {
			message = parser.parse(hl7Text);
			final Group group = message.getParent();

			final Map<String, String> attributes = new HashMap<>();
			extractAttributes(group, attributes);
			flowFile = session.putAllAttributes(flowFile, attributes);
			getLogger().info("Successfully extracted {} attributes for {}; routing to success", new Object[] {attributes.size(), flowFile});
			getLogger().debug("Added the following attributes for {}: {}", new Object[] {flowFile, attributes});
			session.transfer(flowFile, REL_SUCCESS);
		} catch (final HL7Exception e) {
			getLogger().error("Failed to extract attributes from {} due to {}", new Object[] {flowFile, e});
			session.transfer(flowFile, REL_FAILURE);
			return;
		}
	}

	private void extractAttributes(final Group group, final Map<String, String> attributes) throws HL7Exception {
		extractAttributes(group, attributes, new HashMap<String, Integer>());
	}
	
	private void extractAttributes(final Group group, final Map<String, String> attributes, final Map<String, Integer> segmentCounts) throws HL7Exception {
		if ( group.isEmpty() ) {
			return;
		}
		
		final String[] structureNames = group.getNames();
		for ( final String structName : structureNames ) {
			final Structure[] subStructures = group.getAll(structName);

			if ( group.isGroup(structName) ) {
				for ( final Structure subStructure : subStructures ) {
					final Group subGroup = (Group) subStructure;
					extractAttributes(subGroup, attributes, segmentCounts);
				}
			} else {
				for ( final Structure structure : subStructures ) {
					final Segment segment = (Segment) structure	;
					
					final String segmentName = segment.getName();
					Integer segmentNum = segmentCounts.get(segmentName);
					if (segmentNum == null) {
						segmentNum = 1;
						segmentCounts.put(segmentName, 1);
					} else {
						segmentNum++;
						segmentCounts.put(segmentName, segmentNum);
					}
					
					final boolean segmentRepeating = segment.getParent().isRepeating(segment.getName());
					final boolean parentRepeating = (segment.getParent().getParent() != segment.getParent() && segment.getParent().getParent().isRepeating(segment.getParent().getName()));
					final boolean useSegmentIndex = segmentRepeating || parentRepeating;
					
					final Map<String, String> attributeMap = getAttributes(segment, useSegmentIndex ? segmentNum : null);
					attributes.putAll(attributeMap);
				}
			}
		}
	}
	
	
	private Map<String, String> getAttributes(final Segment segment, final Integer segmentNum) throws HL7Exception {
		final Map<String, String> attributes = new HashMap<>();
		
		for (int i=1; i <= segment.numFields(); i++) {
			final String fieldName = segment.getName() + (segmentNum == null ? "" : "_" + segmentNum) + "." + i;
			final Type[] types = segment.getField(i);
			final StringBuilder sb = new StringBuilder();
			for ( final Type type : types ) {
				final String typeValue = getValue(type);
				if ( !typeValue.isEmpty() ) {
					sb.append(typeValue).append("^");
				}
			}
			
			if ( sb.length() == 0 ) {
				continue;
			}
			String typeVal = sb.toString();
			if ( typeVal.endsWith("^") ) {
				typeVal = typeVal.substring(0, typeVal.length() - 1);
			}
			
			attributes.put(fieldName, typeVal);
		}

		return attributes;
	}
	
	
	private String getValue(final Type type) {
		if ( type == null ) {
			return "";
		}
		
		if ( type instanceof Primitive ) {
			final String value = ((Primitive) type).getValue();
			return value == null ? "" : value;
		} else if ( type instanceof Composite ) {
			final StringBuilder sb = new StringBuilder();
			final Composite composite = (Composite) type;
			for ( final Type component : composite.getComponents() ) {
				final String componentValue = getValue(component);
				if ( !componentValue.isEmpty() ) {
					sb.append(componentValue).append("^");
				}
			}
			
			final String value = sb.toString();
			if ( value.endsWith("^") ) {
				return value.substring(0, value.length() - 1);
			}
			
			return value;
		} else if ( type instanceof Varies ) {
			final Varies varies = (Varies) type;
			return getValue(varies.getData());
		}
		
		return "";
	}
}
