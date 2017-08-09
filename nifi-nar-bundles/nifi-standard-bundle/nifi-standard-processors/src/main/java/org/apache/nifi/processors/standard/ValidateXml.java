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
package org.apache.nifi.processors.standard;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.xml.sax.SAXException;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"xml", "schema", "validation", "xsd"})
@WritesAttributes({
    @WritesAttribute(attribute = "validatexml.invalid.error", description = "If the flow file is routed to the invalid relationship "
            + "the attribute will contain the error message resulting from the validation failure.")
})
@CapabilityDescription("Validates the contents of FlowFiles against a user-specified XML Schema file")
public class ValidateXml extends AbstractProcessor {

    public static final String ERROR_ATTRIBUTE_KEY = "validatexml.invalid.error";

    public static final PropertyDescriptor SCHEMA_FILE = new PropertyDescriptor.Builder()
            .name("Schema File")
            .description("The path to the Schema file that is to be used for validation")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final Relationship REL_VALID = new Relationship.Builder()
            .name("valid")
            .description("FlowFiles that are successfully validated against the schema are routed to this relationship")
            .build();
    public static final Relationship REL_INVALID = new Relationship.Builder()
            .name("invalid")
            .description("FlowFiles that are not valid according to the specified schema are routed to this relationship")
            .build();

    private static final String SCHEMA_LANGUAGE = "http://www.w3.org/2001/XMLSchema";

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private final AtomicReference<Schema> schemaRef = new AtomicReference<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SCHEMA_FILE);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_VALID);
        relationships.add(REL_INVALID);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void parseSchema(final ProcessContext context) throws IOException, SAXException {
        try {
            final File file = new File(context.getProperty(SCHEMA_FILE).getValue());
            final SchemaFactory schemaFactory = SchemaFactory.newInstance(SCHEMA_LANGUAGE);
            final Schema schema = schemaFactory.newSchema(file);
            this.schemaRef.set(schema);
        } catch (final SAXException e) {
            throw e;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final List<FlowFile> flowFiles = session.get(50);
        if (flowFiles.isEmpty()) {
            return;
        }

        final Schema schema = schemaRef.get();
        final Validator validator = schema.newValidator();
        final ComponentLog logger = getLogger();

        for (FlowFile flowFile : flowFiles) {
            final AtomicBoolean valid = new AtomicBoolean(true);
            final AtomicReference<Exception> exception = new AtomicReference<Exception>(null);

            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    try {
                        validator.validate(new StreamSource(in));
                    } catch (final IllegalArgumentException | SAXException e) {
                        valid.set(false);
                        exception.set(e);
                    }
                }
            });

            if (valid.get()) {
                logger.debug("Successfully validated {} against schema; routing to 'valid'", new Object[]{flowFile});
                session.getProvenanceReporter().route(flowFile, REL_VALID);
                session.transfer(flowFile, REL_VALID);
            } else {
                flowFile = session.putAttribute(flowFile, ERROR_ATTRIBUTE_KEY, exception.get().getLocalizedMessage());
                logger.info("Failed to validate {} against schema due to {}; routing to 'invalid'", new Object[]{flowFile, exception.get().getLocalizedMessage()});
                session.getProvenanceReporter().route(flowFile, REL_INVALID);
                session.transfer(flowFile, REL_INVALID);
            }
        }
    }
}
