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

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.JsonValidator;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.ValidationMessage;
import com.networknt.schema.SpecVersion.VersionFlag;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"JSON", "schema", "validation"})
@WritesAttributes({
    @WritesAttribute(attribute = "validatejson.invalid.error", description = "If the flow file is routed to the invalid relationship "
            + "the attribute will contain the error message resulting from the validation failure.")
})
@CapabilityDescription("Validates the contents of FlowFiles against a user-specified JSON Schema file")
public class ValidateJson extends AbstractProcessor {

    public static final String ERROR_ATTRIBUTE_KEY = "validatejson.invalid.error";

    public static final AllowableValue SCHEMA_VERSION_4 = new AllowableValue("V4");
    public static final AllowableValue SCHEMA_VERSION_6 = new AllowableValue("V6");
    public static final AllowableValue SCHEMA_VERSION_7 = new AllowableValue("V7");
    public static final AllowableValue SCHEMA_VERSION_V201909 = new AllowableValue("V201909");

    public static final PropertyDescriptor SCHEMA_VERSION = new PropertyDescriptor
        .Builder().name("SCHEMA_VERSION")
        .displayName("Schema Version")
        .description("The JSON schema specification")
        .required(true)
        .allowableValues(SCHEMA_VERSION_4, SCHEMA_VERSION_6, SCHEMA_VERSION_7, SCHEMA_VERSION_V201909)
        .defaultValue(SCHEMA_VERSION_V201909.getValue())
        .build();

    public static final PropertyDescriptor SCHEMA_TEXT = new PropertyDescriptor
        .Builder().name("SCHEMA_TEXT")
        .displayName("Schema Text")
        .description("The text of a JSON schema")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(JsonValidator.INSTANCE)
        .build();

    public static final Relationship REL_VALID = new Relationship.Builder()
        .name("valid")
        .description("FlowFiles that are successfully validated against the schema are routed to this relationship")
        .build();

    public static final Relationship REL_INVALID = new Relationship.Builder()
        .name("invalid")
        .description("FlowFiles that are not valid according to the specified schema are routed to this relationship")
        .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("FlowFiles that cannot be read as JSON are routed to this relationship")
        .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private List<AllowableValue> allowableValues;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(SCHEMA_TEXT);
        descriptors.add(SCHEMA_VERSION);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_VALID);
        relationships.add(REL_INVALID);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<AllowableValue> allowableValues = new ArrayList<AllowableValue>();
        allowableValues.add(SCHEMA_VERSION_4);
        allowableValues.add(SCHEMA_VERSION_6);
        allowableValues.add(SCHEMA_VERSION_7);
        allowableValues.add(SCHEMA_VERSION_V201909);
        this.allowableValues = Collections.unmodifiableList(allowableValues);

    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    private ObjectMapper mapper = new ObjectMapper();
    private VersionFlag schemaVersion;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        // Set JSON schema version to use from processor property
        this.schemaVersion = VersionFlag.V201909;
        if (context.getProperty(SCHEMA_VERSION).getValue() == SCHEMA_VERSION_4.getValue()) {
            this.schemaVersion = VersionFlag.V4;
        }
        if (context.getProperty(SCHEMA_VERSION).getValue() == SCHEMA_VERSION_6.getValue()) {
            this.schemaVersion = VersionFlag.V6;
        }
        if (context.getProperty(SCHEMA_VERSION).getValue() == SCHEMA_VERSION_7.getValue()) {
            this.schemaVersion = VersionFlag.V7;
        }
        if (context.getProperty(SCHEMA_VERSION).getValue() == SCHEMA_VERSION_V201909.getValue()) {
            this.schemaVersion = VersionFlag.V201909;
        }
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        try (InputStream in = session.read(flowFile)) {
            // Read in flowFile inputstream, and validate against schema
            JsonNode node = mapper.readTree(in);
            String schemaText = context.getProperty(SCHEMA_TEXT).evaluateAttributeExpressions().getValue();
            JsonSchemaFactory factory = JsonSchemaFactory.getInstance(schemaVersion);
            JsonSchema schema = factory.getSchema(schemaText);
            Set<ValidationMessage> errors = schema.validate(node);

            if (errors.size() > 0) {
                // Schema checks failed
                flowFile = session.putAttribute(flowFile, ERROR_ATTRIBUTE_KEY, errors.toString());
                this.getLogger().info("Found {} to be invalid when validated against schema; routing to 'invalid'", flowFile);
                session.getProvenanceReporter().route(flowFile, REL_INVALID);
                session.transfer(flowFile, REL_INVALID);
            } else {
                // Schema check passed
                this.getLogger().debug("Successfully validated {} against schema; routing to 'valid'", flowFile);
                session.getProvenanceReporter().route(flowFile, REL_VALID);
                session.transfer(flowFile, REL_VALID);
            }

        } catch (IOException ioe) {
            // Failed to read flowFile
            this.getLogger().error("Failed to process {}, routing to 'failure' details:", flowFile, ioe);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
