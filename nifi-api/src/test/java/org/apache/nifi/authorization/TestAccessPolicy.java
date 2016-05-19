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
package org.apache.nifi.authorization;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestAccessPolicy {

    static final Resource TEST_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return "1";
        }

        @Override
        public String getName() {
            return "resource1";
        }
    };

    @Test
    public void testSimpleCreation() {
        final String identifier = "1";
        final String entity1 = "entity1";
        final String entity2 = "entity2";
        final RequestAction action = RequestAction.READ;

        final AccessPolicy policy = new AccessPolicy.AccessPolicyBuilder()
                .identifier(identifier)
                .resource(TEST_RESOURCE)
                .addEntity(entity1)
                .addEntity(entity2)
                .addAction(action)
                .build();

        assertEquals(identifier, policy.getIdentifier());

        assertNotNull(policy.getResource());
        assertEquals(TEST_RESOURCE.getIdentifier(), policy.getResource().getIdentifier());

        assertNotNull(policy.getEntities());
        assertEquals(2, policy.getEntities().size());
        assertTrue(policy.getEntities().contains(entity1));
        assertTrue(policy.getEntities().contains(entity2));

        assertNotNull(policy.getActions());
        assertEquals(1, policy.getActions().size());
        assertTrue(policy.getActions().contains(action));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingIdentifier() {
        new AccessPolicy.AccessPolicyBuilder()
                .resource(TEST_RESOURCE)
                .addEntity("entity1")
                .addAction(RequestAction.READ)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingResource() {
        new AccessPolicy.AccessPolicyBuilder()
                .identifier("1")
                .addEntity("entity1")
                .addAction(RequestAction.READ)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingEntities() {
        new AccessPolicy.AccessPolicyBuilder()
                .identifier("1")
                .resource(TEST_RESOURCE)
                .addAction(RequestAction.READ)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingActions() {
        new AccessPolicy.AccessPolicyBuilder()
                .identifier("1")
                .resource(TEST_RESOURCE)
                .addEntity("entity1")
                .build();
    }

    @Test
    public void testFromPolicy() {
        final String identifier = "1";
        final String entity1 = "entity1";
        final String entity2 = "entity2";
        final RequestAction action = RequestAction.READ;

        final AccessPolicy policy = new AccessPolicy.AccessPolicyBuilder()
                .identifier(identifier)
                .resource(TEST_RESOURCE)
                .addEntity(entity1)
                .addEntity(entity2)
                .addAction(action)
                .build();

        assertEquals(identifier, policy.getIdentifier());

        assertNotNull(policy.getResource());
        assertEquals(TEST_RESOURCE.getIdentifier(), policy.getResource().getIdentifier());

        assertNotNull(policy.getEntities());
        assertEquals(2, policy.getEntities().size());
        assertTrue(policy.getEntities().contains(entity1));
        assertTrue(policy.getEntities().contains(entity2));

        assertNotNull(policy.getActions());
        assertEquals(1, policy.getActions().size());
        assertTrue(policy.getActions().contains(action));

        final AccessPolicy policy2 = new AccessPolicy.AccessPolicyBuilder(policy).build();
        assertEquals(policy.getIdentifier(), policy2.getIdentifier());
        assertEquals(policy.getResource().getIdentifier(), policy2.getResource().getIdentifier());
        assertEquals(policy.getEntities(), policy2.getEntities());
        assertEquals(policy.getActions(), policy2.getActions());
    }

    @Test(expected = IllegalStateException.class)
    public void testFromPolicyAndChangeIdentifier() {
        final AccessPolicy policy = new AccessPolicy.AccessPolicyBuilder()
                .identifier("1")
                .resource(TEST_RESOURCE)
                .addEntity("entity1")
                .addAction(RequestAction.READ)
                .build();

        new AccessPolicy.AccessPolicyBuilder(policy).identifier("2").build();
    }

    @Test
    public void testAddRemoveClearEntities() {
        final AccessPolicy.AccessPolicyBuilder builder = new AccessPolicy.AccessPolicyBuilder()
                .identifier("1")
                .resource(TEST_RESOURCE)
                .addEntity("entity1")
                .addAction(RequestAction.READ);

        final AccessPolicy policy1 = builder.build();
        assertEquals(1, policy1.getEntities().size());
        assertTrue(policy1.getEntities().contains("entity1"));

        final Set<String> moreEntities = new HashSet<>();
        moreEntities.add("entity2");
        moreEntities.add("entity3");
        moreEntities.add("entity4");

        final AccessPolicy policy2 = builder.addEntities(moreEntities).build();
        assertEquals(4, policy2.getEntities().size());
        assertTrue(policy2.getEntities().contains("entity1"));
        assertTrue(policy2.getEntities().contains("entity2"));
        assertTrue(policy2.getEntities().contains("entity3"));
        assertTrue(policy2.getEntities().contains("entity4"));

        final AccessPolicy policy3 = builder.removeEntity("entity3").build();
        assertEquals(3, policy3.getEntities().size());
        assertTrue(policy3.getEntities().contains("entity1"));
        assertTrue(policy3.getEntities().contains("entity2"));
        assertTrue(policy3.getEntities().contains("entity4"));

        final Set<String> removeEntities = new HashSet<>();
        removeEntities.add("entity1");
        removeEntities.add("entity4");

        final AccessPolicy policy4 = builder.removeEntities(removeEntities).build();
        assertEquals(1, policy4.getEntities().size());
        assertTrue(policy4.getEntities().contains("entity2"));

        try {
            builder.clearEntities().build();
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testAddRemoveClearActions() {
        final AccessPolicy.AccessPolicyBuilder builder = new AccessPolicy.AccessPolicyBuilder()
                .identifier("1")
                .resource(TEST_RESOURCE)
                .addEntity("entity1")
                .addAction(RequestAction.READ);

        final AccessPolicy policy1 = builder.build();
        assertEquals(1, policy1.getActions().size());
        assertTrue(policy1.getActions().contains(RequestAction.READ));

        final AccessPolicy policy2 = builder
                .removeAction(RequestAction.READ)
                .addAction(RequestAction.WRITE)
                .build();

        assertEquals(1, policy2.getActions().size());
        assertTrue(policy2.getActions().contains(RequestAction.WRITE));

        try {
            builder.clearActions().build();
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {

        }
    }

}
