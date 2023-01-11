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
package org.apache.nifi.runtime.manifest;

import org.apache.nifi.extension.manifest.ExtensionManifest;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class ExtensionManifestContainer {

    private final ExtensionManifest manifest;
    private final Map<String, String> additionalDetails;

    public ExtensionManifestContainer(final ExtensionManifest manifest) {
        this(manifest, null);
    }

    public ExtensionManifestContainer(final ExtensionManifest manifest, final Map<String, String> additionalDetails) {
        this.manifest = Objects.requireNonNull(manifest);
        this.additionalDetails = Collections.unmodifiableMap(additionalDetails == null
                ? Collections.emptyMap() : new LinkedHashMap<>(additionalDetails));
    }

    public ExtensionManifest getManifest() {
        return manifest;
    }

    public Map<String, String> getAdditionalDetails() {
        return additionalDetails;
    }
}
