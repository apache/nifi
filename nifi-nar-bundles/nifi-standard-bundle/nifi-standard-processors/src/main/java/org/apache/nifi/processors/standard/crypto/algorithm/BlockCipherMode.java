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
package org.apache.nifi.processors.standard.crypto.algorithm;

/**
 * Cryptographic Block Cipher Mode Cipher enumeration with acronym and description
 */
public enum BlockCipherMode {
    CBC("CBC", "Cipher Block Chaining"),

    CCM("CCM", "Counter with Cipher Block Chaining-Message Authentication Code"),

    CFB("CFB", "Cipher Feedback"),

    ECB("ECB", "Electronic Codebook"),

    GCM("GCM", "Galois Counter Mode"),

    OFB("OFB", "Output Feedback");

    private String label;

    private String description;

    BlockCipherMode(final String label, final String description) {
        this.label = label;
        this.description = description;
    }

    public String getLabel() {
        return label;
    }

    public String getDescription() {
        return description;
    }
}
