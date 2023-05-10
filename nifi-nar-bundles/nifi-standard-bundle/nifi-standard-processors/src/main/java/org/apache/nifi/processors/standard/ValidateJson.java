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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion.VersionFlag;
import com.networknt.schema.ValidationMessage;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"JSON", "schema", "validation"})
@WritesAttributes({
    @WritesAttribute(attribute = ValidateJson.ERROR_ATTRIBUTE_KEY, description = "If the flow file is routed to the invalid relationship "
            + ", this attribute will contain the error message resulting from the validation failure.")
})
@CapabilityDescription("Validates the contents of FlowFiles against a configurable JSON Schema. See json-schema.org for specification standards.")
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "Validating JSON requires reading FlowFile content into memory")
public class ValidateJson extends AbstractProcessor {
    public enum SchemaVersion implements DescribedValue {
        DRAFT_4("Draft Version 4", "Draft 4", VersionFlag.V4),
        DRAFT_6("Draft Version 6", "Draft 6", VersionFlag.V6),
        DRAFT_7("Draft Version 7", "Draft 7", VersionFlag.V7),
        DRAFT_2019_09("Draft Version 2019-09", "Draft 2019-09", VersionFlag.V201909),
        DRAFT_2020_12("Draft Version 2020-12", "Draft 2020-12", VersionFlag.V202012);

        private final String description;
        private final String displayName;
        private final VersionFlag versionFlag;

        SchemaVersion(String description, String displayName, VersionFlag versionFlag) {
            this.description = description;
            this.displayName = displayName;
            this.versionFlag = versionFlag;
        }

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }

        public VersionFlag getVersionFlag() {
            return versionFlag;
        }
    }

    public static final String ERROR_ATTRIBUTE_KEY = "json.validation.errors";

    public static final PropertyDescriptor SCHEMA_CONTENT = new PropertyDescriptor
            .Builder().name("JSON Schema")
            .displayName("JSON Schema")
            .description("The content of a JSON Schema")
            .required(true)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.URL, ResourceType.TEXT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor SCHEMA_VERSION = new PropertyDescriptor
        .Builder().name("Schema Version")
        .displayName("Schema Version")
        .description("The JSON schema specification")
        .required(true)
        .allowableValues(SchemaVersion.class)
        .defaultValue(SchemaVersion.DRAFT_2020_12.getValue())
        .build();

    public static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(
            Arrays.asList(
                    SCHEMA_CONTENT,
                    SCHEMA_VERSION
            )
    );

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

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList(
                    REL_VALID,
                    REL_INVALID,
                    REL_FAILURE
            ))
    );

    private static final ObjectMapper MAPPER;
    static {
        MAPPER = new ObjectMapper();
        MAPPER.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
    }

    private JsonSchema schema;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        try (final InputStream inputStream = context.getProperty(SCHEMA_CONTENT).asResource().read()) {
            final SchemaVersion schemaVersion = SchemaVersion.valueOf(context.getProperty(SCHEMA_VERSION).getValue());
            final JsonSchemaFactory factory = JsonSchemaFactory.getInstance(schemaVersion.getVersionFlag());
            schema = factory.getSchema(inputStream);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try (final InputStream in = session.read(flowFile)) {
            final JsonNode node = MAPPER.readTree(in);
            final Set<ValidationMessage> errors = schema.validate(node);

            if (errors.isEmpty()) {
                getLogger().debug("JSON {} valid", flowFile);
                session.getProvenanceReporter().route(flowFile, REL_VALID);
                session.transfer(flowFile, REL_VALID);
            } else {
                final String validationMessages = errors.toString();
                flowFile = session.putAttribute(flowFile, ERROR_ATTRIBUTE_KEY, validationMessages);
                getLogger().warn("JSON {} invalid: Validation Errors {}", flowFile, validationMessages);
                session.getProvenanceReporter().route(flowFile, REL_INVALID);
                session.transfer(flowFile, REL_INVALID);
            }
        } catch (final Exception e) {
            getLogger().error("JSON processing failed {}", flowFile, e);
            session.getProvenanceReporter().route(flowFile, REL_FAILURE);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
