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
package org.apache.nifi.processors.aws.dynamodb;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class ItemKeysTest {

    @Test
    public void testHashNullRangeNullEquals() {
        ItemKeys ik1 = new ItemKeys(null, null);
        ItemKeys ik2 = new ItemKeys(null, null);
        assertEquals(ik1, ik2);
        assertEquals(ik1.hashCode(), ik2.hashCode());
        assertEquals(ik1.toString(), ik2.toString());
    }

    @Test
    public void testHashNotNullRangeNullEquals() {
        ItemKeys ik1 = new ItemKeys(string("abc"), null);
        ItemKeys ik2 = new ItemKeys(string("abc"), null);
        assertEquals(ik1, ik2);
        assertEquals(ik1.hashCode(), ik2.hashCode());
        assertEquals(ik1.toString(), ik2.toString());
    }

    @Test
    public void testHashNullRangeNotNullEquals() {
        ItemKeys ik1 = new ItemKeys(null, string("ab"));
        ItemKeys ik2 = new ItemKeys(null, string("ab"));
        assertEquals(ik1, ik2);
        assertEquals(ik1.hashCode(), ik2.hashCode());
        assertEquals(ik1.toString(), ik2.toString());
    }

    @Test
    public void testHashNotNullRangeNotNullEquals() {
        ItemKeys ik1 = new ItemKeys(string("abc"), string("pqr"));
        ItemKeys ik2 = new ItemKeys(string("abc"), string("pqr"));
        assertEquals(ik1, ik2);
        assertEquals(ik1.hashCode(), ik2.hashCode());
        assertEquals(ik1.toString(), ik2.toString());
    }

    @Test
    public void testHashNotNullRangeNotNullForOtherNotEquals() {
        ItemKeys ik1 = new ItemKeys(null, string("ab"));
        ItemKeys ik2 = new ItemKeys(string("ab"), null);
        assertNotEquals(ik1, ik2);
    }

    private static AttributeValue string(final String s) {
        return AttributeValue.builder().s(s).build();
    }
}
