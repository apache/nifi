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
package org.apache.nifi.processors.asana;

import org.apache.nifi.processors.asana.utils.AsanaObject;
import org.apache.nifi.processors.asana.utils.AsanaObjectState;
import org.apache.nifi.util.StringUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class AsanaObjectTest {

    private static final String GID = "1234";
    private static final String CONTENT = "Lorem Ipsum";
    private static final String FINGERPRINT = "Foo Bar";

    @Test
    public void testWithoutContent() {
        final AsanaObject asanaObject = new AsanaObject(AsanaObjectState.NEW, GID);

        assertEquals(AsanaObjectState.NEW, asanaObject.getState());
        assertEquals(GID, asanaObject.getGid());
        assertEquals(StringUtils.EMPTY, asanaObject.getContent());
        assertEquals(StringUtils.EMPTY, asanaObject.getFingerprint());
    }

    @Test
    public void testWithoutFingerprint() {
        final AsanaObject asanaObject = new AsanaObject(AsanaObjectState.NEW, GID, CONTENT);

        assertEquals(AsanaObjectState.NEW, asanaObject.getState());
        assertEquals(GID, asanaObject.getGid());
        assertEquals(CONTENT, asanaObject.getContent());
        assertEquals(CONTENT, asanaObject.getFingerprint());
    }

    @Test
    public void testWithFingerprint() {
        final AsanaObject asanaObject = new AsanaObject(AsanaObjectState.NEW, GID, CONTENT, FINGERPRINT);

        assertEquals(AsanaObjectState.NEW, asanaObject.getState());
        assertEquals(GID, asanaObject.getGid());
        assertEquals(CONTENT, asanaObject.getContent());
        assertEquals(FINGERPRINT, asanaObject.getFingerprint());
    }

    @Test
    public void testWithNullFingerprint() {
        final AsanaObject asanaObject = new AsanaObject(AsanaObjectState.NEW, GID, CONTENT, null);

        assertEquals(AsanaObjectState.NEW, asanaObject.getState());
        assertEquals(GID, asanaObject.getGid());
        assertEquals(CONTENT, asanaObject.getContent());
        assertEquals(CONTENT, asanaObject.getFingerprint());
    }

    @Test
    public void testWithNullContentAndNonNullFingerprint() {
        final AsanaObject asanaObject = new AsanaObject(AsanaObjectState.NEW, GID, null, FINGERPRINT);

        assertEquals(AsanaObjectState.NEW, asanaObject.getState());
        assertEquals(GID, asanaObject.getGid());
        assertNull(asanaObject.getContent());
        assertEquals(FINGERPRINT, asanaObject.getFingerprint());
    }

    @Test
    public void testWithOtherStates() {
        assertEquals(AsanaObjectState.UPDATED, new AsanaObject(AsanaObjectState.UPDATED, GID).getState());
        assertEquals(AsanaObjectState.REMOVED, new AsanaObject(AsanaObjectState.REMOVED, GID).getState());

        assertEquals(AsanaObjectState.UPDATED, new AsanaObject(AsanaObjectState.UPDATED, GID, CONTENT).getState());
        assertEquals(AsanaObjectState.REMOVED, new AsanaObject(AsanaObjectState.REMOVED, GID, CONTENT).getState());

        assertEquals(AsanaObjectState.UPDATED, new AsanaObject(AsanaObjectState.UPDATED, GID, CONTENT, FINGERPRINT).getState());
        assertEquals(AsanaObjectState.REMOVED, new AsanaObject(AsanaObjectState.REMOVED, GID, CONTENT, FINGERPRINT).getState());
    }

    @Test
    public void testEquality() {
        final AsanaObject one = new AsanaObject(AsanaObjectState.UPDATED, GID);
        assertEquals(one, one);

        final AsanaObject other = new AsanaObject(AsanaObjectState.NEW, GID);
        assertNotEquals(one, other);

        assertEquals(one, new AsanaObject(AsanaObjectState.UPDATED, GID));
        assertNotEquals(one, new AsanaObject(AsanaObjectState.UPDATED, GID, CONTENT));
        assertNotEquals(one, new AsanaObject(AsanaObjectState.UPDATED, GID, CONTENT, FINGERPRINT));

        final AsanaObject another = new AsanaObject(AsanaObjectState.REMOVED, GID, CONTENT, FINGERPRINT);
        assertEquals(another, another);
        assertEquals(another, new AsanaObject(AsanaObjectState.REMOVED, GID, CONTENT, FINGERPRINT));
        assertNotEquals(another, new AsanaObject(AsanaObjectState.REMOVED, GID, CONTENT + "1", FINGERPRINT));
        assertNotEquals(another, new AsanaObject(AsanaObjectState.REMOVED, GID, CONTENT, FINGERPRINT + "1"));
    }
}
