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
package org.apache.nifi.security.encryption;

import java.util.Map;
import java.util.Objects;

/**
 * Context describing the sensitive value supplied to encryption and decryption operations. Implementations backed by an
 * external key management service can supply the attributes as encryption context and rely on the service to record
 * them for auditing. The same context must be supplied for decryption as was supplied for encryption.
 *
 * @param category Category of the value being protected
 * @param attributes Attributes describing the location of the value, stored as an unmodifiable copy and never null
 */
public record SensitivePropertyContext(
        SensitivePropertyCategory category,
        Map<String, String> attributes
) {
    public SensitivePropertyContext {
        Objects.requireNonNull(category, "Category required");
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }
}
