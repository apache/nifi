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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;


@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"json", "schema", "validation"})
@CapabilityDescription("Validates the contents of FlowFiles against a user-specified JSON Schema file")
public class ValidateJson extends AbstractProcessor {

    public static final PropertyDescriptor SCHEMA_FILE = new PropertyDescriptor.Builder()
            .name("validate-json-schema-file")
            .displayName("Schema File")
            .description("The path to the Schema file that is to be used for validation. Only one of Schema File or Schema Body may be used")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor SCHEMA_BODY = new PropertyDescriptor.Builder()
            .name("validate-json-schema-body")
            .displayName("Schema Body")
            .required(false)
            .description("Json Schema Body that is to be used for validation. Only one of Schema File or Schema Body may be used")
            .expressionLanguageSupported(false)
            .addValidator(Validator.VALID)
            .build();

    public static final Relationship REL_VALID = new Relationship.Builder()
            .name("valid")
            .description("FlowFiles that are successfully validated against the schema are routed to this relationship")
            .build();
    public static final Relationship REL_INVALID = new Relationship.Builder()
            .name("invalid")
            .description("FlowFiles that are not valid according to the specified schema are routed to this relationship")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private final AtomicReference<Schema> schemaRef = new AtomicReference<>();

    /**
     * Custom validation for ensuring exactly one of Script File or Script Body is populated
     *
     * @param validationContext provides a mechanism for obtaining externally
     *                          managed values, such as property values and supplies convenience methods
     *                          for operating on those values
     * @return A collection of validation results
     */
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();

        // Verify that exactly one of "script file" or "script body" is set
        Map<PropertyDescriptor, String> propertyMap = validationContext.getProperties();
        if (StringUtils.isEmpty(propertyMap.get(SCHEMA_FILE)) == StringUtils.isEmpty(propertyMap.get(SCHEMA_BODY))) {
            results.add(new ValidationResult.Builder().valid(false).explanation(
                    "Exactly one of Schema File or Schema Body must be set").build());
        }

        return results;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SCHEMA_FILE);
        properties.add(SCHEMA_BODY);
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
    public void parseSchema(final ProcessContext context) throws IOException {
        JSONObject jsonObjectSchema;
        if(context.getProperty(SCHEMA_FILE).isSet()){
            try(FileInputStream inputStream = new FileInputStream(new File(context.getProperty(SCHEMA_FILE).getValue()))) {
                JSONTokener jsonTokener = new JSONTokener(inputStream);
                jsonObjectSchema = new JSONObject(jsonTokener);
            }
        } else {
            String rawSchema = context.getProperty(SCHEMA_BODY).getValue();
            jsonObjectSchema = new JSONObject(rawSchema);
        }
        Schema schema = SchemaLoader.load(jsonObjectSchema);
        this.schemaRef.set(schema);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final Schema schema = schemaRef.get();
        final ComponentLog logger = getLogger();

        final AtomicBoolean valid = new AtomicBoolean(true);
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) {
                try {
                    String str = IOUtils.toString(in, StandardCharsets.UTF_8);
                    if (str.startsWith("[")) {
                        schema.validate(new JSONArray(str)); // throws a ValidationException if this object is invalid
                    } else {
                        schema.validate(new JSONObject(str)); // throws a ValidationException if this object is invalid
                    }
                } catch (final IllegalArgumentException | ValidationException | IOException e) {
                    valid.set(false);
                    logger.debug("Failed to validate {} against schema due to {}", new Object[]{flowFile, e});
                }
            }
        });

        if (valid.get()) {
            logger.debug("Successfully validated {} against schema; routing to 'valid'", new Object[]{flowFile});
            session.getProvenanceReporter().route(flowFile, REL_VALID);
            session.transfer(flowFile, REL_VALID);
        } else {
            logger.debug("Failed to validate {} against schema; routing to 'invalid'", new Object[]{flowFile});
            session.getProvenanceReporter().route(flowFile, REL_INVALID);
            session.transfer(flowFile, REL_INVALID);
        }
    }
}
