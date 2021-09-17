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
package org.apache.nifi.flow.resource;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;

@RunWith(MockitoJUnitRunner.class)
public class DoNotReplaceResolutionStrategyTest {
    private static final String RESOURCE_NAME = "resource.json";
    private static final String OTHER_RESOURCE_NAME = "driver.jar";
    private static final long MODIFIED_AT = System.currentTimeMillis();

    @Mock
    private File targetDirectory;

    @Mock
    private File existingResource;

    private final DoNotReplaceResolutionStrategy testSubject = new DoNotReplaceResolutionStrategy();
    private final FlowResourceDescriptor descriptor = new ImmutableFlowResourceDescriptor(RESOURCE_NAME, MODIFIED_AT);

    @Test
    public void testEmptyFolder() {
        // given
        Mockito.when(targetDirectory.listFiles()).thenReturn(new File[]{});

        // when
        final boolean result = testSubject.shouldBeFetched(targetDirectory, descriptor);

        // then
        Assert.assertTrue(result);
    }

    @Test
    public void testFolderWithoutMatchingFile() {
        // given
        Mockito.when(existingResource.getName()).thenReturn(OTHER_RESOURCE_NAME);
        Mockito.when(targetDirectory.listFiles()).thenReturn(new File[]{existingResource});

        // when
        final boolean result = testSubject.shouldBeFetched(targetDirectory, descriptor);

        // then
        Assert.assertTrue(result);
    }

    @Test
    public void testFolderWithMatchingFile() {
        // given
        Mockito.when(existingResource.getName()).thenReturn(RESOURCE_NAME);
        Mockito.when(targetDirectory.listFiles()).thenReturn(new File[]{existingResource});

        // when
        final boolean result = testSubject.shouldBeFetched(targetDirectory, descriptor);

        // then
        Assert.assertFalse(result);
    }
}