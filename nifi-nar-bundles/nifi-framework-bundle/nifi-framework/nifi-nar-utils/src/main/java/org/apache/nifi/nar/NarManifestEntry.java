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
package org.apache.nifi.nar;

/**
 * Enumeration of entries that will be in a NAR MANIFEST file.
 */
public enum NarManifestEntry {

    NAR_GROUP("Nar-Group"),
    NAR_ID("Nar-Id"),
    NAR_VERSION("Nar-Version"),
    NAR_DEPENDENCY_GROUP("Nar-Dependency-Group"),
    NAR_DEPENDENCY_ID("Nar-Dependency-Id"),
    NAR_DEPENDENCY_VERSION("Nar-Dependency-Version"),
    BUILD_TAG("Build-Tag"),
    BUILD_REVISION("Build-Revision"),
    BUILD_BRANCH("Build-Branch"),
    BUILD_TIMESTAMP("Build-Timestamp"),
    BUILD_JDK("Build-Jdk"),
    BUILT_BY("Built-By"),
    ;

    final String manifestName;

    NarManifestEntry(String manifestName) {
        this.manifestName = manifestName;
    }

    public String getManifestName() {
        return manifestName;
    }

}
