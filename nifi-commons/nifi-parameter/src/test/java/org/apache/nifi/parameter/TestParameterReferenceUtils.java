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
package org.apache.nifi.parameter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestParameterReferenceUtils {

    @Test
    public void testExtractOneToOneParameterReferenceMatches() {
        assertEquals("foo", ParameterReferenceUtils.extractOneToOneParameterReference("#{foo}"));
        assertEquals("a-b_c.d", ParameterReferenceUtils.extractOneToOneParameterReference("#{a-b_c.d}"));
        assertEquals("My Parameter", ParameterReferenceUtils.extractOneToOneParameterReference("#{'My Parameter'}"));
        assertEquals("with.dot", ParameterReferenceUtils.extractOneToOneParameterReference("#{'with.dot'}"));
    }

    @Test
    public void testExtractOneToOneParameterReferenceRejectsNonOneToOneValues() {
        assertNull(ParameterReferenceUtils.extractOneToOneParameterReference(null));
        assertNull(ParameterReferenceUtils.extractOneToOneParameterReference(""));
        assertNull(ParameterReferenceUtils.extractOneToOneParameterReference("plain text"));
        assertNull(ParameterReferenceUtils.extractOneToOneParameterReference("prefix #{foo}"));
        assertNull(ParameterReferenceUtils.extractOneToOneParameterReference("#{foo}suffix"));
        assertNull(ParameterReferenceUtils.extractOneToOneParameterReference("#{a}#{b}"));
        assertNull(ParameterReferenceUtils.extractOneToOneParameterReference("##{foo}"));
        assertNull(ParameterReferenceUtils.extractOneToOneParameterReference("#{abc"));
        assertNull(ParameterReferenceUtils.extractOneToOneParameterReference("#{a}b}}"));
        assertNull(ParameterReferenceUtils.extractOneToOneParameterReference("${#{foo}}"));
    }
}
