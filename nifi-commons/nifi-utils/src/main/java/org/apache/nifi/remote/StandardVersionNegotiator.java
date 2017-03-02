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
package org.apache.nifi.remote;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class StandardVersionNegotiator implements VersionNegotiator {

    private final List<Integer> versions;
    private int curVersion;

    public StandardVersionNegotiator(final int... supportedVersions) {
        if (Objects.requireNonNull(supportedVersions).length == 0) {
            throw new IllegalArgumentException("At least one version must be supported");
        }

        final List<Integer> supported = new ArrayList<>();
        for (final int version : supportedVersions) {
            supported.add(version);
        }
        this.versions = Collections.unmodifiableList(supported);
        this.curVersion = supportedVersions[0];
    }

    @Override
    public int getVersion() {
        return curVersion;
    }

    @Override
    public void setVersion(final int version) throws IllegalArgumentException {
        if (!isVersionSupported(version)) {
            throw new IllegalArgumentException("Version " + version + " is not supported");
        }

        this.curVersion = version;
    }

    @Override
    public int getPreferredVersion() {
        return versions.get(0);
    }

    @Override
    public Integer getPreferredVersion(final int maxVersion) {
        for (final Integer version : this.versions) {
            if (maxVersion >= version) {
                return version;
            }
        }
        return null;
    }

    @Override
    public boolean isVersionSupported(final int version) {
        return versions.contains(version);
    }

    @Override
    public List<Integer> getSupportedVersions() {
        return versions;
    }

}
