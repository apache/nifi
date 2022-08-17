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

import org.apache.nifi.c2.protocol.component.api.BuildInfo;
import org.apache.nifi.c2.protocol.component.api.Bundle;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;
import org.apache.nifi.c2.protocol.component.api.SchedulingDefaults;
import org.apache.nifi.extension.manifest.ExtensionManifest;

/**
 * Builder for creating a RuntimeManifest.
 */
public interface RuntimeManifestBuilder {

    /**
     * @param identifier the identifier for the manifest
     * @return the builder
     */
    RuntimeManifestBuilder identifier(String identifier);

    /**
     * @param version the version for the manifest
     * @return the builder
     */
    RuntimeManifestBuilder version(String version);

    /**
     * @param runtimeType the runtime type (i.e. nifi, nifi-stateless, minifi-cpp, etc)
     * @return the builder
     */
    RuntimeManifestBuilder runtimeType(String runtimeType);

    /**
     * @param buildInfo the build info for the manifest
     * @return the builder
     */
    RuntimeManifestBuilder buildInfo(BuildInfo buildInfo);

    /**
     * Adds a Bundle from the given ExtensionManifest.
     *
     * @param extensionManifest the extension manifest to add
     * @return the builder
     */
    RuntimeManifestBuilder addBundle(ExtensionManifest extensionManifest);

    /**
     * Adds a Bundle for each of the given ExtensionManifests.
     *
     * @param extensionManifests the extension manifests to add
     * @return the builder
     */
    RuntimeManifestBuilder addBundles(Iterable<ExtensionManifest> extensionManifests);

    /**
     * Adds the given Bundle.
     *
     * @param bundle the bundle to add
     * @return the builder
     */
    RuntimeManifestBuilder addBundle(Bundle bundle);

    /**
     * @param schedulingDefaults the scheduling defaults
     * @return the builder
     */
    RuntimeManifestBuilder schedulingDefaults(SchedulingDefaults schedulingDefaults);

    /**
     * @return a RuntimeManifest containing the added bundles
     */
    RuntimeManifest build();

}
