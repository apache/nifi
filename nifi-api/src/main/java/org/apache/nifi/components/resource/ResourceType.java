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

package org.apache.nifi.components.resource;

public enum ResourceType {
    /**
     * Referenced Resource is a File on a local (or mounted) file system
     */
    FILE("file"),

    /**
     * Referenced Resource is a directory on a local (or mounted) file system
     */
    DIRECTORY("directory"),

    /**
     * Referenced Resource is UTF-8 text, rather than an external entity
     */
    TEXT("text"),

    /**
     * Referenced Resource is a URL that uses the HTTP, HTTPS, or file protocol
     * (i.e., <code>http://...</code>, <code>https://...</code>, or <code>file:...</code>)
     */
    URL("URL");

    private final String prettyPrintName;
    ResourceType(final String prettyPrintName) {
        this.prettyPrintName = prettyPrintName;
    }

    @Override
    public String toString() {
        return prettyPrintName;
    }
}
