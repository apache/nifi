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
import java.util.Optional;

/**
 * Basic abstraction over Protobuf schema message entity.
 * It only represents the fields that are needed for crude schema insights. It's useful when the
 * message name resolver needs to pinpoint specific messages by message indexes encoded on the wire.
 * <p>
 * <a href="https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format">See the Confluent protobuf wire format.</a>
 * <p>
 * It contains bare minimum of information for name resolver to be able to resolve message names
 */
public interface ProtobufMessageSchema {

    String getName();

    Optional<String> getPackageName();

    default boolean isDefaultPackage() {
        return getPackageName().isEmpty();
    }

    List<ProtobufMessageSchema> getChildMessageSchemas();
}
