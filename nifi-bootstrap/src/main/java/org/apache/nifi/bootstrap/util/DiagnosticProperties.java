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
package org.apache.nifi.bootstrap.util;

public class DiagnosticProperties {

    private final String dirPath;
    private final int maxFileCount;
    private final int maxSizeInBytes;
    private final boolean verbose;
    private final boolean allowed;

    public DiagnosticProperties(String dirPath, int maxFileCount, int maxSizeInBytes, boolean verbose, boolean allowed) {
        this.dirPath = dirPath;
        this.maxFileCount = maxFileCount;
        this.maxSizeInBytes = maxSizeInBytes;
        this.verbose = verbose;
        this.allowed = allowed;
    }

    public String getDirPath() {
        return dirPath;
    }

    public int getMaxFileCount() {
        return maxFileCount;
    }

    public int getMaxSizeInBytes() {
        return maxSizeInBytes;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public boolean isAllowed() {
        return allowed;
    }
}
