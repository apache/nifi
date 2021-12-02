/*
 * Apache NiFi
 * Copyright 2014-2018 The Apache Software Foundation
 *
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

package org.apache.nifi.c2.protocol.api;

public enum ManifestStrategy {
    STATIC("Static",
        "This strategy performs a static mapping of agent classes to a specific manifest id"),
    FIRST_IN("First In",
        "This is the default manifest resolution strategy and will bind an agent class to the first manifest reported for it."),
    LAST_IN("Last In",
        "This strategy updates the associated manifest to the most recently reported by any agent associated with the specified class.");

    private final String description;
    private final String displayName;


    ManifestStrategy(final String displayName, final String description) {
        this.description = description;
        this.displayName = displayName;
    }

    public String getDescription() {
        return this.description;
    }

    public String getDisplayName() {
        return this.displayName;
    }

    public static ManifestStrategy getDefault() {
        return FIRST_IN;
    }
}
