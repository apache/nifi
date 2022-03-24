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

package org.apache.nifi.repository.schema;

import java.util.Map;

public enum FieldType {
    STRING(2, true, String.class),    // 2 bytes for length of string
    LONG_STRING(4, true, String.class),   // 4 bytes for length of string
    BOOLEAN(1, false, Boolean.class),
    LONG(8, false, Long.class),
    INT(4, false, Integer.class),
    BYTE_ARRAY(4, true, byte[].class),    // 4 bytes for number of bytes
    COMPLEX(2, true, Record.class),   // 2 bytes for number of sub-elements
    MAP(2, true, Map.class),
    UNION(4, false, NamedValue.class);


    private final int numBytes;
    private final boolean lengthEncoded;
    private final Class<?> javaClass;

    private FieldType(final int numBytes, final boolean lengthEncoded, final Class<?> javaClass) {
        this.numBytes = numBytes;
        this.lengthEncoded = lengthEncoded;
        this.javaClass = javaClass;
    }


    /**
     * Indicates the number of bytes that must be read for this field. If this field is length-encoded
     * (see {@link #isLengthEncoded()}, then this method tells us how many bytes to read in order to determine
     * the full length of the field. Otherwise, these bytes tell us the full length of the field themselves.
     *
     * @return the number of bytes to read for this field
     */
    public int getNumberOfBytes() {
        return numBytes;
    }

    /**
     * This method returns <code>true</code>, then the value of {@link #getNumberOfBytes()} tells us how many bytes to read in
     * order to determine the full length of the field (if the field is not complex) or the number of sub-fields to
     * read in order to determine the full field (if this field is complex). If <code>false</code>, the value of
     * {@link #getNumberOfBytes()} simply tells us how many bytes must be read in order to read the entire field.
     *
     * @return whether or not the field is length-encoded.
     */
    public boolean isLengthEncoded() {
        return lengthEncoded;
    }

    /**
     * @return the Java type that corresponds to this FieldType
     */
    public Class<?> getJavaClass() {
        return javaClass;
    }
}
