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
package org.apache.nifi.uuid5;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Uuid5UtilTest {
    private static final String NAMESPACE = "11111111-1111-1111-1111-111111111111";

    private static final String NAME = "identifier";

    private static final String NAME_UUID_NULL_NAMESPACE = "485f3c34-2c6c-50a3-9904-63233467b96a";

    private static final String NAME_UUID_NAMESPACE = "c9b54096-3890-53cc-b375-af7e7ec5e59f";

    @Test
    void testFromStringNullNamespace() {
        final String uuid = Uuid5Util.fromString(NAME, null);

        assertEquals(NAME_UUID_NULL_NAMESPACE, uuid);
    }

    @Test
    void testFromStringNamespace() {
        final String uuid = Uuid5Util.fromString(NAME, NAMESPACE);

        assertEquals(NAME_UUID_NAMESPACE, uuid);
    }
}
