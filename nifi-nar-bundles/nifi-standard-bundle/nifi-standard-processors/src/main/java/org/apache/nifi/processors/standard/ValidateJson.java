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
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.JsonSchema;
import org.apache.nifi.schema.access.JsonSchemaRegistryComponent;
import org.apache.nifi.schema.access.SchemaVersion;
import org.apache.nifi.schemaregistry.services.JsonSchemaRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;


@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"JSON", "schema", "validation"})
@WritesAttributes({
    @WritesAttribute(attribute = ValidateJson.ERROR_ATTRIBUTE_KEY, description = "If the flow file is routed to the invalid relationship "
            + ", this attribute will contain the error message resulting from the validation failure.")
})
@CapabilityDescription("Validates the contents of FlowFiles against a configurable JSON Schema. See json-schema.org for specification standards. " +
        "This Processor does not support input containing multiple JSON objects, such as newline-delimited JSON. If the input FlowFile contains " +
        "newline-delimited JSON, only the first line will be validated."
)
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "Validating JSON requires reading FlowFile content into memory")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.REFERENCE_REMOTE_RESOURCES,
                        explanation = "Schema configuration can reference resources over HTTP"
                )
        }
)
public class ValidateJson extends AbstractProcessor {

    public static final String SCHEMA_NAME_PROPERTY_NAME = "Schema Name";
    public static final String SCHEMA_CONTENT_PROPERTY_NAME = "JSON Schema";
    public static final String ERROR_ATTRIBUTE_KEY = "json.validation.errors";
    public static final AllowableValue SCHEMA_NAME_PROPERTY = new AllowableValue("schema-name", "Use '" + SCHEMA_NAME_PROPERTY_NAME + "' Property",
            "The name of the Schema to use is specified by the '" + SCHEMA_NAME_PROPERTY_NAME +
                    "' Property. The value of this property is used to lookup the Schema in the configured JSON Schema Registry service.");
    public static final AllowableValue SCHEMA_CONTENT_PROPERTY = new AllowableValue("schema-content-property", "Use '" + SCHEMA_CONTENT_PROPERTY_NAME + "' Property",
            "A URL or file path to the JSON schema or the actual JSON schema is specified by the '" + SCHEMA_CONTENT_PROPERTY_NAME + "' Property. " +
                    "No matter how the JSON schema is specified, it must be a valid JSON schema");

    private static final List<AllowableValue> STRATEGY_LIST = Arrays.asList(SCHEMA_NAME_PROPERTY, SCHEMA_CONTENT_PROPERTY);

    public static final PropertyDescriptor SCHEMA_ACCESS_STRATEGY = new PropertyDescriptor.Builder()
            .name("schema-access-strategy")
            .displayName("Schema Access Strategy")
            .description("Specifies how to obtain the schema that is to be used for interpreting the data.")
            .allowableValues(STRATEGY_LIST.toArray(new AllowableValue[0]))
            .defaultValue(SCHEMA_CONTENT_PROPERTY.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
            .name("schema-name")
            .displayName(SCHEMA_NAME_PROPERTY_NAME)
            .description("Specifies the name of the schema to lookup in the Schema Registry property")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${schema.name}")
            .dependsOn(SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY)
            .required(false)
            .build();

    public static final PropertyDescriptor SCHEMA_REGISTRY = new PropertyDescriptor.Builder()
            .name("schema-registry")
            .displayName("Schema Registry")
            .description("Specifies the Controller Service to use for the Schema Registry")
            .identifiesControllerService(JsonSchemaRegistry.class)
            .required(false)
            .dependsOn(SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY)
            .build();

    public static final PropertyDescriptor SCHEMA_CONTENT = new PropertyDescriptor.Builder()
            .name(SCHEMA_CONTENT_PROPERTY_NAME)
            .displayName(SCHEMA_CONTENT_PROPERTY_NAME)
            .description("A URL/file path to the JSON schema or the actual JSON schema content")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.URL, ResourceType.TEXT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(SCHEMA_ACCESS_STRATEGY, SCHEMA_CONTENT_PROPERTY)
            .build();

    public static final PropertyDescriptor SCHEMA_VERSION = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(JsonSchemaRegistryComponent.SCHEMA_VERSION)
            .required(false)
            .dependsOn(SCHEMA_ACCESS_STRATEGY, SCHEMA_CONTENT_PROPERTY)
            .build();

    public static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(
            Arrays.asList(
                    SCHEMA_ACCESS_STRATEGY,
                    SCHEMA_NAME,
                    SCHEMA_REGISTRY,
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

    private final ConcurrentMap<SchemaVersion, JsonSchemaFactory> schemaFactories =  Arrays.stream(SchemaVersion.values())
            .collect(Collectors.toConcurrentMap(Function.identity(),
                    schemaDraftVersion -> JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.fromId(schemaDraftVersion.getUri()).get())));
    private volatile com.networknt.schema.JsonSchema schema;
    private volatile JsonSchemaRegistry jsonSchemaRegistry;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> validationResults = new ArrayList<>();
        final String schemaAccessStrategy = getSchemaAccessStrategy(validationContext);

        if (isNameStrategy(validationContext) && !validationContext.getProperty(SCHEMA_REGISTRY).isSet()) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(SCHEMA_REGISTRY.getDisplayName())
                    .explanation(getPropertyValidateMessage(schemaAccessStrategy, SCHEMA_REGISTRY))
                    .valid(false)
                    .build());
        } else if (isContentStrategy(validationContext) && !validationContext.getProperty(SCHEMA_CONTENT).isSet()) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(SCHEMA_CONTENT.getDisplayName())
                    .explanation(getPropertyValidateMessage(schemaAccessStrategy, SCHEMA_CONTENT))
                    .valid(false)
                    .build());
        }

        return validationResults;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        if (isNameStrategy(context)) {
            jsonSchemaRegistry = context.getProperty(SCHEMA_REGISTRY).asControllerService(JsonSchemaRegistry.class);
        } else if (isContentStrategy(context)) {
            try (final InputStream inputStream = context.getProperty(SCHEMA_CONTENT).asResource().read()) {
                final SchemaVersion schemaVersion = SchemaVersion.valueOf(context.getProperty(SCHEMA_VERSION).getValue());
                final JsonSchemaFactory factory = schemaFactories.get(schemaVersion);
                schema = factory.getSchema(inputStream);
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        if(isNameStrategy(context)) {
            try {
                final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions(flowFile).getValue();
                final JsonSchema jsonSchema = jsonSchemaRegistry.retrieveSchema(schemaName);
                final JsonSchemaFactory factory = schemaFactories.get(jsonSchema.getSchemaDraftVersion());
                schema = factory.getSchema(jsonSchema.getSchemaText());
            } catch (Exception e) {
                getLogger().error("Could not retrieve JSON schema for {}", flowFile, e);
                session.getProvenanceReporter().route(flowFile, REL_FAILURE);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
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

    private String getPropertyValidateMessage(String schemaAccessStrategy, PropertyDescriptor property) {
        return "The '" + schemaAccessStrategy + "' Schema Access Strategy requires that the " + property.getDisplayName() + " property be set.";
    }

    private boolean isNameStrategy(PropertyContext context) {
        final String schemaAccessStrategy = getSchemaAccessStrategy(context);
        return SCHEMA_NAME_PROPERTY.getValue().equals(schemaAccessStrategy);
    }

    private String getSchemaAccessStrategy(PropertyContext context) {
        return context.getProperty(SCHEMA_ACCESS_STRATEGY).getValue();
    }

    private boolean isContentStrategy(PropertyContext context) {
        final String schemaAccessStrategy = getSchemaAccessStrategy(context);
        return SCHEMA_CONTENT_PROPERTY.getValue().equals(schemaAccessStrategy);
    }
}
