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
package org.apache.nifi.schemaregistry.services;

import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Simple implementation of SchemaDefinition that holds a schema identifier, text, and references.
 */
public class StandardSchemaDefinition implements SchemaDefinition {

    private final SchemaIdentifier identifier;
    private final String text;
    private final Map<String, SchemaDefinition> references;
    private final SchemaType schemaType;

    /**
     * Creates a new StandardSchemaDefinition.
     *
     * @param identifier the schema identifier
     * @param text       the schema text content
     * @param references map of schema references (key = reference name, value = referenced schema)
     */
    public StandardSchemaDefinition(final SchemaIdentifier identifier,
                                    final String text,
                                    final SchemaType schemaType,
                                    final Map<String, SchemaDefinition> references

    ) {
        this.identifier = requireNonNull(identifier, "Schema identifier cannot be null");
        this.text = requireNonNull(text, "Schema text cannot be null");
        this.schemaType = requireNonNull(schemaType, "Schema type cannot be null");
        this.references = references != null ? Map.copyOf(references) : Map.of();
    }

    /**
     * Creates a new StandardSchemaDefinition without references.
     *
     * @param identifier the schema identifier
     * @param text       the schema text content
     */
    public StandardSchemaDefinition(final SchemaIdentifier identifier, final String text, final SchemaType schemaType) {
        this(identifier, text, schemaType, null);
    }

    @Override
    public SchemaType getSchemaType() {
        return schemaType;
    }

    @Override
    public SchemaIdentifier getIdentifier() {
        return identifier;
    }

    @Override
    public String getText() {
        return text;
    }

    @Override
    public Map<String, SchemaDefinition> getReferences() {
        return references;
    }

    @Override
    public final boolean equals(final Object o) {
        if (!(o instanceof final StandardSchemaDefinition that)) {
            return false;
        }

        return identifier.equals(that.identifier) && text.equals(that.text) && references.equals(that.references) && schemaType == that.schemaType;
    }

    @Override
    public int hashCode() {
        int result = identifier.hashCode();
        result = 31 * result + text.hashCode();
        result = 31 * result + references.hashCode();
        result = 31 * result + schemaType.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "StandardSchemaDefinition{" +
            "identifier=" + identifier +
            ", references=" + references +
            ", schemaType=" + schemaType +
            '}';
    }
}
