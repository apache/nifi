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
package org.apache.nifi.xml.processing;

import javax.xml.XMLConstants;

/**
 * XML Processing Features
 */
public enum ProcessingFeature {
    /** Secure Processing */
    SECURE_PROCESSING(XMLConstants.FEATURE_SECURE_PROCESSING, true),

    /** SAX Namespaces */
    SAX_NAMESPACES("http://xml.org/sax/features/namespaces", true),

    /** SAX Namespace Prefixes */
    SAX_NAMESPACE_PREFIXES("http://xml.org/sax/features/namespace-prefixes", true),

    /** Disallow Document Type Declaration */
    DISALLOW_DOCTYPE_DECL("http://apache.org/xml/features/disallow-doctype-decl", true);

    private final String feature;

    private final boolean enabled;

    ProcessingFeature(final String feature, final boolean enabled) {
        this.feature = feature;
        this.enabled = enabled;
    }

    public String getFeature() {
        return feature;
    }

    public boolean isEnabled() {
        return enabled;
    }
}
