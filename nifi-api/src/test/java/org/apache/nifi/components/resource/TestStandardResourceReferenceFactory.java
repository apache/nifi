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

package org.apache.nifi.components.resource;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestStandardResourceReferenceFactory {

    private final StandardResourceReferenceFactory subject = new StandardResourceReferenceFactory();

    @Test
    public void testCreateResourceReferences() {
        String value = "/dir1/test1.jar,/dir2/test2.jar";
        ResourceDefinition resourceDefinition = createResourceDefinition();

        ResourceReferences resourceReferences = subject.createResourceReferences(value, resourceDefinition);

        assertNotNull(resourceReferences);

        List<ResourceReference> resourceReferencesList = resourceReferences.asList();
        assertNotNull(resourceReferencesList);
        assertEquals(2, resourceReferencesList.size());

        assertResourceReference(resourceReferencesList.get(0), "/dir1/test1.jar");
        assertResourceReference(resourceReferencesList.get(1), "/dir2/test2.jar");
    }

    @Test
    public void testCreateResourceReferencesWhenValueIsNull() {
        String value = null;
        ResourceDefinition resourceDefinition = createResourceDefinition();

        ResourceReferences resourceReferences = subject.createResourceReferences(value, resourceDefinition);

        assertEmptyResourceReferences(resourceReferences);
    }

    @Test
    public void testCreateResourceReferencesWhenValueIsEmpty() {
        String value = "";
        ResourceDefinition resourceDefinition = createResourceDefinition();

        ResourceReferences resourceReferences = subject.createResourceReferences(value, resourceDefinition);

        assertEmptyResourceReferences(resourceReferences);
    }
    @Test
    public void testCreateResourceReferencesWhenResourceDefinitionIsNull() {
        String value = "/dir1/test1.jar";
        ResourceDefinition resourceDefinition = null;

        ResourceReferences resourceReferences = subject.createResourceReferences(value, resourceDefinition);

        assertEmptyResourceReferences(resourceReferences);
    }

    private StandardResourceDefinition createResourceDefinition() {
        return new StandardResourceDefinition(ResourceCardinality.SINGLE, Collections.singleton(ResourceType.FILE));
    }

    private void assertResourceReference(ResourceReference resourceReference, String location) {
        assertEquals(new File(location).getAbsolutePath(), resourceReference.getLocation());
        assertEquals(ResourceType.FILE, resourceReference.getResourceType());
    }

    private void assertEmptyResourceReferences(ResourceReferences resourceReferences) {
        assertNotNull(resourceReferences);
        assertNotNull(resourceReferences.asList());
        assertTrue(resourceReferences.asList().isEmpty());
    }
}
