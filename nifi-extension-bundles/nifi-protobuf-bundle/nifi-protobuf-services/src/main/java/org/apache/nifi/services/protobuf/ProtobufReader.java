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
package org.apache.nifi.services.protobuf;

import com.squareup.wire.schema.CoreLoaderKt;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.Schema;
import com.squareup.wire.schema.SchemaLoader;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.services.protobuf.schema.ProtoSchemaStrategy;
import org.apache.nifi.services.protobuf.validation.ProtoValidationResource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"protobuf", "record", "reader", "parser"})
@CapabilityDescription("Parses a Protocol Buffers message from binary format.")
@SeeAlso(StandardProtobufReader.class)
public class ProtobufReader extends SchemaRegistryService implements RecordReaderFactory {

    private static final String ANY_PROTO = "google/protobuf/any.proto";
    private static final String DURATION_PROTO = "google/protobuf/duration.proto";
    private static final String EMPTY_PROTO = "google/protobuf/empty.proto";
    private static final String STRUCT_PROTO = "google/protobuf/struct.proto";
    private static final String TIMESTAMP_PROTO = "google/protobuf/timestamp.proto";
    private static final String WRAPPERS_PROTO = "google/protobuf/wrappers.proto";

    private static final AllowableValue GENERATE_FROM_PROTO_FILE = new AllowableValue("generate-from-proto-file",
            "Generate from Proto file", "The record schema is generated from the provided proto file");

    private volatile String messageType;
    private volatile Schema protoSchema;

    // Holder of cached proto information so validation does not reload the same proto file over and over
    private final AtomicReference<ProtoValidationResource> validationResourceHolder = new AtomicReference<>();

    public static final PropertyDescriptor PROTOBUF_DIRECTORY = new PropertyDescriptor.Builder()
            .name("Proto Directory")
            .description("Directory containing Protocol Buffers message definition (.proto) file(s).")
            .required(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MESSAGE_TYPE = new PropertyDescriptor.Builder()
            .name("Message Type")
            .description("Fully qualified name of the Protocol Buffers message type including its package (eg. mypackage.MyMessage). " +
                    "The .proto files configured in '" + PROTOBUF_DIRECTORY.getDisplayName() + "' must contain the definition of this message type.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(PROTOBUF_DIRECTORY);
        properties.add(MESSAGE_TYPE);
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>();
        final String protoDirectory = validationContext.getProperty(PROTOBUF_DIRECTORY).evaluateAttributeExpressions().getValue();
        final String messageType = validationContext.getProperty(MESSAGE_TYPE).evaluateAttributeExpressions().getValue();

        if (protoDirectory != null && messageType != null) {
            final Schema protoSchema = getSchemaForValidation(protoDirectory);
            if (protoSchema.getType(messageType) == null) {
                problems.add(new ValidationResult.Builder()
                        .subject(MESSAGE_TYPE.getDisplayName())
                        .valid(false)
                        .explanation(String.format("'%s' message type cannot be found in the provided proto files.", messageType))
                        .build());
            }
        }

        return problems;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final String protoDirectory = context.getProperty(PROTOBUF_DIRECTORY).evaluateAttributeExpressions().getValue();
        messageType = context.getProperty(MESSAGE_TYPE).evaluateAttributeExpressions().getValue();
        protoSchema = loadProtoSchema(protoDirectory);
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String allowableValue, final SchemaRegistry schemaRegistry, final PropertyContext context) {
        if (allowableValue.equalsIgnoreCase(GENERATE_FROM_PROTO_FILE.getValue())) {
            return new ProtoSchemaStrategy(messageType, protoSchema);
        }

        return SchemaAccessUtils.getSchemaAccessStrategy(allowableValue, schemaRegistry, context);
    }

    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>(super.getSchemaAccessStrategyValues());
        allowableValues.add(GENERATE_FROM_PROTO_FILE);
        return allowableValues;
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return GENERATE_FROM_PROTO_FILE;
    }

    @Override
    public RecordReader createRecordReader(Map<String, String> variables, InputStream in, long inputLength, ComponentLog logger) throws IOException, SchemaNotFoundException {
        return new ProtobufRecordReader(protoSchema, messageType, in, getSchema(variables, in, null));
    }

    private Schema loadProtoSchema(final String protoDirectory) {
        final SchemaLoader schemaLoader = new SchemaLoader(FileSystems.getDefault());
        schemaLoader.initRoots(Arrays.asList(Location.get(protoDirectory),
                Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, ANY_PROTO),
                Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, DURATION_PROTO),
                Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, EMPTY_PROTO),
                Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, STRUCT_PROTO),
                Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, TIMESTAMP_PROTO),
                Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, WRAPPERS_PROTO)), Collections.emptyList());
        return schemaLoader.loadSchema();
    }

    private Schema getSchemaForValidation(final String protoDirectory) {
        ProtoValidationResource validationResource = validationResourceHolder.get();
        if (validationResource == null || !protoDirectory.equals(validationResource.getProtoDirectory())) {
            validationResource = new ProtoValidationResource(protoDirectory, loadProtoSchema(protoDirectory));
            validationResourceHolder.set(validationResource);
        }

        return validationResource.getProtoSchema();
    }
}
