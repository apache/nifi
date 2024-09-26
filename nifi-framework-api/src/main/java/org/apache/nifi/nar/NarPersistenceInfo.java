/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.nar;

import java.io.File;
import java.util.Objects;

/**
 * The information about a NAR persisted by a {@link NarPersistenceProvider}.
 */
public class NarPersistenceInfo {

    private final File narFile;
    private final NarProperties narProperties;

    public NarPersistenceInfo(final File narFile, final NarProperties narProperties) {
        this.narFile = Objects.requireNonNull(narFile);
        this.narProperties = Objects.requireNonNull(narProperties);
    }

    public File getNarFile() {
        return narFile;
    }

    public NarProperties getNarProperties() {
        return narProperties;
    }

    @Override
    public String toString() {
        return "%s:%s:%s".formatted(narProperties.getNarGroup(), narProperties.getNarId(), narProperties.getNarVersion());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final NarPersistenceInfo that = (NarPersistenceInfo) o;
        return Objects.equals(narProperties.getCoordinate(), that.narProperties.getCoordinate());
    }

    @Override
    public int hashCode() {
        return Objects.hash(narProperties.getCoordinate());
    }
}
