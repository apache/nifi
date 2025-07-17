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
import java.util.Objects;
import java.util.Optional;

/**
 * Basic abstraction over Protobuf schema message entity.
 * It contains only the fields that are needed for crude schema insights. It's useful when the
 * message name resolver needs to pinpoint specific messages by message indexes encoded on the wire.
 * <p>
 * <a href="https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format">See the Confluent protobuf wire format.</a>
 * <p>
 * It contains bare minimum of information for name resolver to be able to resolve message names
 */
public final class ProtobufMessageSchema {

    private final String name;
    private final Optional<String> packageName;
    private final List<ProtobufMessageSchema> childMessageSchemas;

    public ProtobufMessageSchema(final String name, final Optional<String> packageName, final List<ProtobufMessageSchema> childMessageSchemas) {
        this.name = name;
        this.packageName = packageName;
        this.childMessageSchemas = childMessageSchemas;
    }

    public String getName() {
        return name;
    }

    public Optional<String> getPackageName() {
        return packageName;
    }

    public boolean isDefaultPackage() {
        return packageName.isEmpty();
    }

    public List<ProtobufMessageSchema> getChildMessageSchemas() {
        return childMessageSchemas;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        final ProtobufMessageSchema that = (ProtobufMessageSchema) obj;
        return Objects.equals(this.name, that.name) && Objects.equals(this.packageName, that.packageName) && Objects.equals(this.childMessageSchemas, that.childMessageSchemas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, packageName, childMessageSchemas);
    }

    @Override
    public String toString() {
        return "ProtobufMessageSchema[" + "name=" + name + ", " + "packageName=" + packageName + ", " + "childMessageSchemas=" + childMessageSchemas + ']';
    }


}
