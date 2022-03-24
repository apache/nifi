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
package org.apache.nifi.flowfile.attributes;

/**
 * Enumeration of standard Flow File Media Types
 */
public enum StandardFlowFileMediaType implements FlowFileMediaType {
    VERSION_1("application/flowfile-v1"),

    VERSION_2("application/flowfile-v2"),

    VERSION_3("application/flowfile-v3"),

    VERSION_UNSPECIFIED("application/flowfile");

    private String mediaType;

    StandardFlowFileMediaType(final String mediaType) {
        this.mediaType = mediaType;
    }

    @Override
    public String getMediaType() {
        return mediaType;
    }
}
