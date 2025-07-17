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
package org.apache.nifi.confluent.schema;

import java.util.List;

/**
 * Parser for Protocol Buffer message schemas.
 * <p>
 * This interface defines the contract for parsing protobuf schema text and extracting
 * individual message schema definitions from it.
 * </p>
 */
public interface ProtobufMessageSchemaParser {

    /**
     * Parses the provided protobuf schema text and extracts all message schema definitions.
     *
     * @param schemaText the protobuf schema text to parse, must not be null
     * @return a list of parsed protobuf message schemas, never null but may be empty if no messages are found
     */
    List<ProtobufMessageSchema> parse(final String schemaText);
}
