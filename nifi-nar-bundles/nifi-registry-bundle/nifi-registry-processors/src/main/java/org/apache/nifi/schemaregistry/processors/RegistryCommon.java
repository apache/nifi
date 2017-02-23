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
package org.apache.nifi.schemaregistry.processors;

import org.apache.avro.Schema;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schemaregistry.processors.BaseTransformer.InvocationContextProperties;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;

/**
 * Strategy that encapsulates common properties and functionalities used by all
 * processors that integrate with Schema Registry.
 */
interface RegistryCommon {

    static final String SCHEMA_ATTRIBUTE_NAME = "schema.text";

    static final PropertyDescriptor REGISTRY_SERVICE = new PropertyDescriptor.Builder()
            .name("schema-registry-service")
            .displayName("Schema Registry Service")
            .description("The Schema Registry Service for serializing/deserializing messages as well as schema retrieval.")
            .required(true)
            .identifiesControllerService(SchemaRegistry.class)
            .build();

    static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
            .name("schema-name")
            .displayName("Schema Name")
            .description("The name of schema.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    static final PropertyDescriptor SCHEMA_TYPE = new PropertyDescriptor.Builder()
            .name("schema-type")
            .displayName("Schema Type")
            .description("The type of schema (avro is the the only current supported schema).")
            .required(true)
            .allowableValues("avro")
            .defaultValue("avro")
            .expressionLanguageSupported(true)
            .build();

    static final PropertyDescriptor DELIMITER = new PropertyDescriptor.Builder()
            .name("csv-delimiter")
            .displayName("CSV delimiter")
            .description("Delimiter character for CSV records")
            .addValidator(CSVUtils.CHAR_VALIDATOR)
            .defaultValue(",")
            .build();

    static final PropertyDescriptor QUOTE = new PropertyDescriptor.Builder()
            .name("csv-quote-character")
            .displayName("CSV quote character")
            .description("Quote character for CSV values")
            .addValidator(CSVUtils.CHAR_VALIDATOR)
            .defaultValue("\"")
            .build();
    /**
     * Utility operation to retrieve and parse {@link Schema} from Schema
     * Registry using provided {@link SchemaRegistry};
     */
    static Schema retrieveSchema(SchemaRegistry schemaRegistry, InvocationContextProperties contextProperties) {
        String schemaName = contextProperties.getPropertyValue(SCHEMA_NAME, true);
        String schemaText = schemaRegistry.retrieveSchemaText(schemaName);
        return new Schema.Parser().parse(schemaText);
    }
}
