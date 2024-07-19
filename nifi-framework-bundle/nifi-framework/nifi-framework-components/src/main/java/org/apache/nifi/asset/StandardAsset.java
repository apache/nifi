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

package org.apache.nifi.asset;

import java.io.File;
import java.util.Objects;
import java.util.Optional;

public class StandardAsset implements Asset {
    private final String identifier;
    private final String parameterContextIdentifier;
    private final String name;
    private final File file;
    private final String digest;

    public StandardAsset(final String identifier, final String paramContextIdentifier, final String name, final File file, final String digest) {
        this.identifier = Objects.requireNonNull(identifier, "Identifier is required");
        this.parameterContextIdentifier = Objects.requireNonNull(paramContextIdentifier, "Parameter Context Identifier is required");
        this.name = Objects.requireNonNull(name, "Name is required");
        this.file = Objects.requireNonNull(file, "File is required");
        this.digest = digest;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getParameterContextIdentifier() {
        return parameterContextIdentifier;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public File getFile() {
        return file;
    }

    @Override
    public Optional<String> getDigest() {
        return Optional.ofNullable(digest);
    }
}
