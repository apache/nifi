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
package org.apache.nifi.manifest;

import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;

import java.io.File;
import java.util.Map;

/**
 * Produces a RuntimeManifest for the current NiFi instance.
 */
public interface RuntimeManifestService {

    /**
     * @return the RuntimeManifest
     */
    RuntimeManifest getManifest();

    /**
     * Returns a RuntimeManifest that is only populated with the components from the
     * specified bundle. If the bundle is not found, there will be no components.
     *
     * @param group The bundle group
     * @param artifact The bundle artifact
     * @param version The bundle version
     * @return the RuntimeManifest
     */
    RuntimeManifest getManifestForBundle(String group, String artifact, String version);

    /**
     * Returns a mapping of additionalDetails for the speicfied bundle.
     *
     * @param group The bundle group
     * @param artifact The bundle artifact
     * @param version The bundle version
     * @return The additionaDetails mapping
     */
    Map<String, File> discoverAdditionalDetails(String group, String artifact, String version);
}