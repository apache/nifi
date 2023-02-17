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
package org.apache.nifi.web.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.nifi.util.ComponentIdGenerator;
import org.junit.Test;

public class SnippetUtilsTest {

    /*
     * This test validates condition where component is being replicated across
     * the cluster
     */
    @Test
    public void validateWithSameSeedSameInceptionIdSameInstanceId() throws Exception {
        Method generateIdMethod = SnippetUtils.class.getDeclaredMethod("generateId", String.class, String.class,
                boolean.class);
        generateIdMethod.setAccessible(true);

        SnippetUtils utils = new SnippetUtils();
        String currentId = ComponentIdGenerator.generateId().toString();
        String seed = ComponentIdGenerator.generateId().toString();
        String id1 = (String) generateIdMethod.invoke(utils, currentId, seed, true);
        String id2 = (String) generateIdMethod.invoke(utils, currentId, seed, true);
        String id3 = (String) generateIdMethod.invoke(utils, currentId, seed, true);
        assertEquals(id1, id2);
        assertEquals(id2, id3);
    }

    /*
     * This test validates condition where components that are being copy/pasted from
     * one another are now replicated across the cluster. Such components will
     * have different inception id (msb) yet different instance id (lsb). The id
     * of these components must be different yet their msb must be the same.
     */
    @Test
    public void validateWithSameSeedSameInceptionIdNotSameInstanceIdIsCopySet() throws Exception {
        Method generateIdMethod = SnippetUtils.class.getDeclaredMethod("generateId", String.class, String.class,
                boolean.class);
        generateIdMethod.setAccessible(true);

        SnippetUtils utils = new SnippetUtils();
        String seed = ComponentIdGenerator.generateId().toString();

        UUID rootId = ComponentIdGenerator.generateId();

        String id1 = (String) generateIdMethod.invoke(utils,
                new UUID(rootId.getMostSignificantBits(), ComponentIdGenerator.generateId().getLeastSignificantBits()).toString(),
                seed, true);
        String id2 = (String) generateIdMethod.invoke(utils,
                new UUID(rootId.getMostSignificantBits(), ComponentIdGenerator.generateId().getLeastSignificantBits()).toString(),
                seed, true);
        String id3 = (String) generateIdMethod.invoke(utils,
                new UUID(rootId.getMostSignificantBits(), ComponentIdGenerator.generateId().getLeastSignificantBits()).toString(),
                seed, true);
        assertNotEquals(id1, id2);
        assertNotEquals(id2, id3);
        UUID uuid1 = UUID.fromString(id1);
        UUID uuid2 = UUID.fromString(id2);
        UUID uuid3 = UUID.fromString(id3);
        // below simply validates that generated UUID is type-one, since timestamp() operation will result
        // in exception if generated UUID is not type-one
        uuid1.timestamp();
        uuid2.timestamp();
        uuid3.timestamp();
        assertNotEquals(uuid1.getMostSignificantBits(), uuid2.getMostSignificantBits());
        assertNotEquals(uuid2.getMostSignificantBits(), uuid3.getMostSignificantBits());
    }

    /*
     * This test validates condition where components that are being re-created
     * from template are now replicated across the cluster. Such components will
     * have the same inception id (msb) yet different instance id (lsb). The id
     * of these components must be different yet their msb must be the same.
     */
    @Test
    public void validateWithSameSeedSameInceptionIdNotSameInstanceIdIsCopyNotSet() throws Exception {
        Method generateIdMethod = SnippetUtils.class.getDeclaredMethod("generateId", String.class, String.class,
                boolean.class);
        generateIdMethod.setAccessible(true);

        SnippetUtils utils = new SnippetUtils();
        String seed = ComponentIdGenerator.generateId().toString();

        UUID rootId = ComponentIdGenerator.generateId();

        String id1 = (String) generateIdMethod.invoke(utils,
                new UUID(rootId.getMostSignificantBits(), ComponentIdGenerator.generateId().getLeastSignificantBits()).toString(),
                seed, false);
        String id2 = (String) generateIdMethod.invoke(utils,
                new UUID(rootId.getMostSignificantBits(), ComponentIdGenerator.generateId().getLeastSignificantBits()).toString(),
                seed, false);
        String id3 = (String) generateIdMethod.invoke(utils,
                new UUID(rootId.getMostSignificantBits(), ComponentIdGenerator.generateId().getLeastSignificantBits()).toString(),
                seed, false);
        assertNotEquals(id1, id2);
        assertNotEquals(id2, id3);
        UUID uuid1 = UUID.fromString(id1);
        UUID uuid2 = UUID.fromString(id2);
        UUID uuid3 = UUID.fromString(id3);
        // below simply validates that generated UUID is type-one, since timestamp() operation will result
        // in exception if generated UUID is not type-one
        uuid1.timestamp();
        uuid2.timestamp();
        uuid3.timestamp();
        assertEquals(uuid1.getMostSignificantBits(), uuid2.getMostSignificantBits());
        assertEquals(uuid2.getMostSignificantBits(), uuid3.getMostSignificantBits());
    }

    /*
     * This test validates condition where components are being copied from one
     * another. The ids of each components must be completely different (msb and
     * lsb) yet each subsequent msb must be > then previous component's msb.
     */
    @Test
    public void validateWithoutSeedSameCurrentIdIsCopySet() throws Exception {
        Method generateIdMethod = SnippetUtils.class.getDeclaredMethod("generateId", String.class, String.class,
                boolean.class);
        generateIdMethod.setAccessible(true);

        boolean isCopy = true;

        SnippetUtils utils = new SnippetUtils();
        String currentId = ComponentIdGenerator.generateId().toString();
        UUID id1 = UUID.fromString((String) generateIdMethod.invoke(utils, currentId, null, isCopy));
        UUID id2 = UUID.fromString((String) generateIdMethod.invoke(utils, currentId, null, isCopy));
        UUID id3 = UUID.fromString((String) generateIdMethod.invoke(utils, currentId, null, isCopy));
        // below simply validates that generated UUID is type-one, since timestamp() operation will result
        // in exception if generated UUID is not type-one
        id1.timestamp();
        id2.timestamp();
        id3.timestamp();
        assertTrue(id1.getMostSignificantBits() < id2.getMostSignificantBits());
        assertTrue(id2.getMostSignificantBits() < id3.getMostSignificantBits());
    }

    /*
     * This test validates condition where new components are being created from
     * existing components such as from imported templates. In this case their
     * instance id (lsb) is irrelevant and new instance id would have to be
     * generated every time yet its inception id (msb) must remain the same.
     */
    @Test
    public void validateWithoutSeedSameCurrentIdIsCopyNotSet() throws Exception {
        Method generateIdMethod = SnippetUtils.class.getDeclaredMethod("generateId", String.class, String.class,
                boolean.class);
        generateIdMethod.setAccessible(true);

        boolean isCopy = false;

        SnippetUtils utils = new SnippetUtils();
        String currentId = ComponentIdGenerator.generateId().toString();
        UUID id1 = UUID.fromString((String) generateIdMethod.invoke(utils, currentId, null, isCopy));
        UUID id2 = UUID.fromString((String) generateIdMethod.invoke(utils, currentId, null, isCopy));
        UUID id3 = UUID.fromString((String) generateIdMethod.invoke(utils, currentId, null, isCopy));
        // below simply validates that generated UUID is type-one, since timestamp() operation will result
        // in exception if generated UUID is not type-one
        id1.timestamp();
        id2.timestamp();
        id3.timestamp();
        assertEquals(id1.getMostSignificantBits(), id2.getMostSignificantBits());
        assertEquals(id2.getMostSignificantBits(), id3.getMostSignificantBits());
    }

    /*
     * This test simply validates that generated IDs are comparable and
     * sequentially correct where each subsequent ID is > previous ID.
     */
    @Test
    public void validateIdOrdering() throws Exception {
        UUID seed = ComponentIdGenerator.generateId();
        UUID currentId1 = ComponentIdGenerator.generateId();
        UUID currentId2 = ComponentIdGenerator.generateId();
        UUID currentId3 = ComponentIdGenerator.generateId();

        UUID id1 = new UUID(currentId1.getMostSignificantBits(),
                UUID.nameUUIDFromBytes((currentId1.toString() + seed.toString()).getBytes(StandardCharsets.UTF_8))
                        .getLeastSignificantBits());
        UUID id2 = new UUID(currentId2.getMostSignificantBits(),
                UUID.nameUUIDFromBytes((currentId2.toString() + seed.toString()).getBytes(StandardCharsets.UTF_8))
                        .getLeastSignificantBits());
        UUID id3 = new UUID(currentId3.getMostSignificantBits(),
                UUID.nameUUIDFromBytes((currentId3.toString() + seed.toString()).getBytes(StandardCharsets.UTF_8))
                        .getLeastSignificantBits());
        List<UUID> list = new ArrayList<>();
        list.add(id2);
        list.add(id3);
        list.add(id1);
        Collections.sort(list);
        assertEquals(id1, list.get(0));
        assertEquals(id2, list.get(1));
        assertEquals(id3, list.get(2));
    }
}
