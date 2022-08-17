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

import org.apache.nifi.util.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

public class ReplaceWithNewerResolutionStrategyTest {
    private static final String TARGET_DIRECTORY = "src/test/resources/";
    private static final String OTHER_TARGET_DIRECTORY = TARGET_DIRECTORY + "t1";
    private static final String RESOURCE_NAME = "resource.json";
    private static final String OTHER_RESOURCE_NAME = "driver.jar";

    private final ReplaceWithNewerResolutionStrategy testSubject = new ReplaceWithNewerResolutionStrategy();


    @Test
    public void testEmptyFolder() throws IOException {
        // given
        final File targetDirectory = new File(OTHER_TARGET_DIRECTORY);
        final ExternalResourceDescriptor descriptor = new ImmutableExternalResourceDescriptor(RESOURCE_NAME, System.currentTimeMillis());
        FileUtils.ensureDirectoryExistAndCanReadAndWrite(targetDirectory);

        // when
        final boolean result = testSubject.shouldBeFetched(targetDirectory, descriptor);

        // then
        Assertions.assertTrue(result);
        FileUtils.deleteFile(targetDirectory, true);
    }

    @Test
    public void testFolderWithoutMatchingFile() {
        // given
        final File targetDirectory = new File(TARGET_DIRECTORY);
        final ExternalResourceDescriptor descriptor = new ImmutableExternalResourceDescriptor(OTHER_RESOURCE_NAME, System.currentTimeMillis());

        // when
        final boolean result = testSubject.shouldBeFetched(targetDirectory, descriptor);

        // then
        Assertions.assertTrue(result);
    }

    @Test
    public void testFolderWithMatchingFileWhenNoNewerCandidate() {
        // given
        final File targetDirectory = new File(TARGET_DIRECTORY);
        final File existingResource = new File(targetDirectory, RESOURCE_NAME);
        final ExternalResourceDescriptor descriptor = new ImmutableExternalResourceDescriptor(RESOURCE_NAME, existingResource.lastModified() - 1);

        // when
        final boolean result = testSubject.shouldBeFetched(targetDirectory, descriptor);

        // then
        Assertions.assertFalse(result);
    }

    @Test
    public void testFolderWithMatchingFileWhenTheCandidateHasTheSameAge() {
        // given
        final File targetDirectory = new File(TARGET_DIRECTORY);
        final File existingResource = new File(targetDirectory, RESOURCE_NAME);
        final ExternalResourceDescriptor descriptor = new ImmutableExternalResourceDescriptor(RESOURCE_NAME, existingResource.lastModified());

        // when
        final boolean result = testSubject.shouldBeFetched(targetDirectory, descriptor);

        // then
        Assertions.assertFalse(result);
    }

    @Test
    public void testFolderWithMatchingFileWhenThereIsNewerCandidate() {
        // given
        final File targetDirectory = new File(TARGET_DIRECTORY);
        final File existingResource = new File(targetDirectory, RESOURCE_NAME);
        final ExternalResourceDescriptor descriptor = new ImmutableExternalResourceDescriptor(RESOURCE_NAME, existingResource.lastModified() + 1);

        // when
        final boolean result = testSubject.shouldBeFetched(targetDirectory, descriptor);

        // then
        Assertions.assertTrue(result);
    }
}