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

import java.util.Optional;

/**
 * Represents a message name in a schema registry service, providing access to both the simple name
 * and the namespace for message types. This interface is typically used for schema formats
 * that support namespaced message types, such as Protocol Buffers, where messages can be organized
 * within packages.
 */
public interface MessageName {

    /**
     * Returns the simple name of the message without any package qualification.
     *
     * @return the message name
     */
    String getName();

    /**
     * Returns the namespace that contains this message, if present.
     *
     * @return an Optional containing the namespace name, or empty if the message
     * is not contained within a namespace
     */
    Optional<String> getNamespace();

    /**
     * Returns the fully qualified name of the message by combining the namespace
     * and simple name. If no namespace is present, returns just the simple name.
     *
     * @return the fully qualified message name in the format "namespace.name" or
     * just "name" if no namespace is present
     */
    default String getFullyQualifiedName() {
        return getNamespace()
            .map(namespace -> namespace + ".")
            .orElse("") + getName();
    }
}
