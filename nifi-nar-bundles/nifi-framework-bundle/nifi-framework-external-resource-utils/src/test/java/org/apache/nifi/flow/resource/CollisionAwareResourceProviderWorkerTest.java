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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


@ExtendWith(MockitoExtension.class)
public class CollisionAwareResourceProviderWorkerTest {
    private static final String PREFIX = "prefix";
    private static final String RESOURCE_NAME_1 = "config.json";
    private static final String RESOURCE_NAME_2 = "config.xml";
    private static final String CONTENT_LOCAL = "lorem";
    private static final String CONTENT_PROVIDED = "ipsum";
    private static final String CONTENT_TEMP = "tmp";
    private static final String TARGET_DIRECTORY_PATH = "target/providerTest";
    private static final File TARGET_DIRECTORY = new File(TARGET_DIRECTORY_PATH);

    @Mock
    private ExternalResourceConflictResolutionStrategy conflictResolutionStrategy;

    private Actions actions;
    private CountDownLatch countDownLatch;
    private TestExternalResourceProvider provider;
    private CollisionAwareResourceProviderWorker testSubject;

    @BeforeEach
    public void setUp() throws IOException {
        if (TARGET_DIRECTORY.exists()) {
            FileUtils.deleteFile(TARGET_DIRECTORY, true);
        }
    }

    @AfterAll
    public static void tearDown() throws IOException {
        if (TARGET_DIRECTORY.exists()) {
            FileUtils.deleteFile(TARGET_DIRECTORY, true);
        }
    }

    @Test
    public void testHappyPathWhenNoResourcesFound() throws InterruptedException {
        setActions(Action.LIST, Action.LIST, Action.LIST);
        setUpProviderWithoutResource();
        setUpTestSubject();

        runningTestSubject();

        assertOperationsAreFinished();
        verifyConflictResulotionWasNotNeeded();
        assertTargetFolderIsEmpty();
    }

    @Test
    public void testRunWithPreExistingDirectory() throws InterruptedException, IOException {
        setActions(Action.LIST);
        ensureTargetDirectoryExists(true);
        setUpProviderWithoutResource();
        setUpTestSubject();

        runningTestSubject();

        assertOperationsAreFinished();
        assertTargetFolderIsEmpty();
    }

    @Test
    public void testRunWithPreExistingDirectoryWhenMissingPrivileges() throws InterruptedException, IOException {
        setNoAction();
        ensureTargetDirectoryExists(false);
        setUpProviderWithoutResource();
        setUpTestSubject();

        runningTestSubject();

        assertOperationsAreFinished();
    }

    @Test
    public void testHappyPathWhenThereAreResources() throws InterruptedException, IOException {
        setUpConflictResolution(true);
        setActions(Action.LIST, Action.FETCH, Action.FETCH);
        final ExternalResourceDescriptor resource1 = getAvailableResource(RESOURCE_NAME_1);
        final ExternalResourceDescriptor resource2 = getAvailableResource(RESOURCE_NAME_2);
        setProviderWithResource(resource1, resource2);
        setUpTestSubject();

        runningTestSubject();

        assertOperationsAreFinished();
        verifyConflictResulotionWasNeeded(resource1, 1);
        verifyConflictResulotionWasNeeded(resource2, 1);
        assertTargetFolderContains(CONTENT_PROVIDED, resource1, resource2);
    }

    @Test
    public void testWhenSomeResourcesAreConflicting() throws InterruptedException, IOException {
        setUpConflictResolution(true);
        setActions(Action.LIST, Action.FETCH);
        final ExternalResourceDescriptor resource1 = getAvailableResource(RESOURCE_NAME_1);
        final ExternalResourceDescriptor resource2 = getAvailableResource(RESOURCE_NAME_2);
        setProviderWithResource(resource1, resource2);
        setResourceShouldNotBeFetched(resource1);
        setUpTestSubject();

        runningTestSubject();

        waitForFileOperationsToFinish();
        verifyConflictResulotionWasNeeded(resource1, 1);
        verifyConflictResulotionWasNeeded(resource2, 1);
        assertTargetFolderContains(CONTENT_PROVIDED, resource2);
    }

    @Test
    public void testRetryAfterFailure() throws InterruptedException, IOException {
        setUpConflictResolution(true);
        setActions(Action.LIST, Action.FETCH, Action.LIST, Action.FETCH);
        final ExternalResourceDescriptor resource1 = getAvailableResource(RESOURCE_NAME_1);
        setUpProviderWithResourceAndFetchSuccessSequence(resource1, false, true);
        setUpTestSubject();

        runningTestSubject();

        waitForFileOperationsToFinish();
        verifyConflictResulotionWasNeeded(resource1, 2);
        assertTargetFolderContains(CONTENT_PROVIDED, resource1);
    }

    @Test
    public void testExistingFileIsReplaced() throws InterruptedException, IOException {
        setUpConflictResolution(true);
        setActions(Action.LIST, Action.FETCH);
        ensureTargetDirectoryExists(true);
        ensureFileExists(RESOURCE_NAME_1, CONTENT_LOCAL,true);
        final ExternalResourceDescriptor resource = getAvailableResource(RESOURCE_NAME_1);
        setProviderWithResource(resource);
        setUpTestSubject();

        runningTestSubject();

        waitForFileOperationsToFinish();
        assertTargetFolderContains(CONTENT_PROVIDED, resource);
    }

    @Test
    @Disabled("This test needs human interaction and should not part of a build")
    public void testExistingFileCannotBeDeleted() throws InterruptedException, IOException {
        setActions(Action.LIST, Action.FETCH);
        ensureTargetDirectoryExists(true);
        ensureFileExists(RESOURCE_NAME_1, CONTENT_LOCAL, false);
        final ExternalResourceDescriptor resource = getAvailableResource(RESOURCE_NAME_1);
        setProviderWithResource(resource);
        setUpTestSubject();

        runningTestSubject();

        waitForFileOperationsToFinish();
        assertTargetFolderContains(CONTENT_LOCAL, resource);
        ensureGivenPathIsAccessible(RESOURCE_NAME_1);
    }

    @Test
    @Disabled("This test needs human interaction and should not part of a build")
    public void testExistingTemporaryFileCannotBeDeleted() throws InterruptedException, IOException {
        setActions(Action.LIST, Action.FETCH);
        ensureTargetDirectoryExists(true);
        final ExternalResourceDescriptor resource = getAvailableResource(RESOURCE_NAME_1);
        setProviderWithResource(resource);
        setUpTestSubject();
        final String tempFileName = setTempFileName();
        ensureFileExists(tempFileName, CONTENT_TEMP,false);

        runningTestSubject();

        waitForFileOperationsToFinish();
        verifyConflictResulotionWasNeeded(resource, 1);
        assertTargetFolderContains(CONTENT_TEMP, tempFileName);
        ensureGivenPathIsAccessible(tempFileName);
    }

    private void setNoAction() {
        setActions();
    }

    private void setActions(Action... actions) {
        this.actions = new Actions(actions);
    }

    private void setUpConflictResolution(final boolean shouldBeFetched) {
        Mockito.when(conflictResolutionStrategy.shouldBeFetched(Mockito.any(File.class), Mockito.any(ExternalResourceDescriptor.class))).thenReturn(shouldBeFetched);
    }

    private void ensureTargetDirectoryExists(boolean accessible) throws IOException {
        FileUtils.ensureDirectoryExistAndCanReadAndWrite(TARGET_DIRECTORY);

        if (!accessible) {
            TARGET_DIRECTORY.setWritable(false);
        }
    }

    private ExternalResourceDescriptor getAvailableResource(String resourceName) {
        final ExternalResourceDescriptor resource = new ImmutableExternalResourceDescriptor(resourceName, System.currentTimeMillis());
        return resource;
    }

    private void setUpTestSubject() {
        testSubject = new CollisionAwareResourceProviderWorker(PREFIX, this.getClass().getClassLoader(), provider, conflictResolutionStrategy, TARGET_DIRECTORY, 50, new CountDownLatch(0));
    }

    private void setUpProviderWithResourceAndFetchSuccessSequence(final ExternalResourceDescriptor resource, final Boolean... fetchSuccessSequence) {
        countDownLatch = new CountDownLatch(actions.numberOfAllActions());
        provider = new TestExternalResourceProvider(Collections.singletonList(resource), countDownLatch, Arrays.asList(fetchSuccessSequence));
    }

    private void setProviderWithResource(final ExternalResourceDescriptor... resources) {
        countDownLatch = new CountDownLatch(actions.numberOfAllActions());
        provider = new TestExternalResourceProvider(Arrays.asList(resources), countDownLatch);
    }

    private void setUpProviderWithoutResource() {
        setProviderWithResource();
    }

    private void ensureFileExists(final String fileName, final String content, final boolean accessible) throws IOException {
        final File file = new File(TARGET_DIRECTORY, fileName);
        file.createNewFile();

        try (
            final FileOutputStream fileOutputStream = new FileOutputStream(file);
            final BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
        ) {
            bufferedOutputStream.write(content.getBytes(StandardCharsets.UTF_8));
        }

        if (!accessible) {
            file.setWritable(false);
        }
    }

    private String setTempFileName() {
        return ".provider_" + testSubject.getName().substring(PREFIX.length() + 3) + ".tmp";
    }

    private void setResourceShouldNotBeFetched(final ExternalResourceDescriptor resource) {
        Mockito.when(conflictResolutionStrategy.shouldBeFetched(TARGET_DIRECTORY, resource)).thenReturn(false);
    }

    private void runningTestSubject() throws InterruptedException {
        final Thread workerThread = new Thread(testSubject);
        workerThread.start();
        countDownLatch.await();
        testSubject.stop();
    }

    private void assertFileContains(final File file, final String expectedContent) throws IOException {
        try (
            final FileReader fileReader = new FileReader(file);
            final BufferedReader bufferedReader = new BufferedReader(fileReader)
        ) {
            final StringBuilder result = new StringBuilder();
            String line = "";

            while ((line = bufferedReader.readLine()) != null) {
                result.append(line);
            }

            Assertions.assertEquals(expectedContent, result.toString());
        }
    }

    private void assertOperationsAreFinished() throws InterruptedException {
        waitForFileOperationsToFinish();
        Assertions.assertFalse(testSubject.isRunning());
        Assertions.assertEquals(actions.numberOfListings(), provider.getListCounter());
        Assertions.assertEquals(actions.numberOfFetches(), provider.getFetchCounter());
    }

    private void verifyConflictResulotionWasNotNeeded() {
        Mockito.verify(conflictResolutionStrategy, Mockito.never()).shouldBeFetched(Mockito.any(File.class), Mockito.any(ExternalResourceDescriptor.class));
    }

    private void verifyConflictResulotionWasNeeded(final ExternalResourceDescriptor resource, final int times) {
        Mockito.verify(conflictResolutionStrategy, Mockito.times(times)).shouldBeFetched(TARGET_DIRECTORY, resource);
    }

    private void assertTargetFolderIsEmpty() {
        Assertions.assertEquals(0, TARGET_DIRECTORY.list().length);
    }

    private void waitForFileOperationsToFinish() throws InterruptedException {
        Thread.sleep(150);
    }

    private void assertTargetFolderContains(final String content, final ExternalResourceDescriptor... resources) throws IOException {
        Assertions.assertEquals(resources.length, TARGET_DIRECTORY.list().length);

        for (final ExternalResourceDescriptor resource : resources) {
            File acquiredResource = new File(TARGET_DIRECTORY_PATH, resource.getLocation());
            Assertions.assertTrue(acquiredResource.exists());
            assertFileContains(acquiredResource, content);
        }
    }

    private void assertTargetFolderContains(final String content, final ExternalResourceDescriptor resource) throws IOException {
        assertTargetFolderContains(content, resource.getLocation());
    }

    private void assertTargetFolderContains(final String content, final String fileName) throws IOException {
        Assertions.assertEquals(1, TARGET_DIRECTORY.list().length);
        File acquiredResource = new File(TARGET_DIRECTORY_PATH, fileName);
        Assertions.assertTrue(acquiredResource.exists());
        assertFileContains(acquiredResource, content);
    }

    private void ensureGivenPathIsAccessible(final String path) {
        final File file = new File(path);
        file.setWritable(true);
    }

    private enum Action {
        LIST, FETCH
    }

    private class Actions {
        private final List<Action> actions;

        Actions(final Action... actions) {
            this.actions = Arrays.asList(actions);
        }

        int numberOfAllActions() {
            return actions.size();
        }

        int numberOfListings() {
            return actions.stream().filter(a -> a.equals(Action.LIST)).collect(Collectors.toList()).size();
        }

        int numberOfFetches() {
            return actions.stream().filter(a -> a.equals(Action.FETCH)).collect(Collectors.toList()).size();
        }
    }

    private static class TestExternalResourceProvider implements ExternalResourceProvider {
        private final List<ExternalResourceDescriptor> resources;
        private final Optional<CountDownLatch> countDownLatch;
        private final List<Boolean> fetchSuccessSequence;

        private final AtomicInteger listCounter = new AtomicInteger(0);
        private final AtomicInteger fetchCounter = new AtomicInteger(0);

        private TestExternalResourceProvider(final List<ExternalResourceDescriptor> resources, final CountDownLatch countDownLatch) {
            this.countDownLatch = Optional.of(countDownLatch);
            this.resources = resources;
            this.fetchSuccessSequence = Collections.singletonList(true);
        }

        private TestExternalResourceProvider(final List<ExternalResourceDescriptor> resources, final CountDownLatch countDownLatch, List<Boolean> fetchSuccessSequence) {
            this.countDownLatch = Optional.of(countDownLatch);
            this.resources = resources;
            this.fetchSuccessSequence = fetchSuccessSequence;
        }

        @Override
        public void initialize(final ExternalResourceProviderInitializationContext context) {

        }

        @Override
        public Collection<ExternalResourceDescriptor> listResources() {
            listCounter.incrementAndGet();
            countDownLatch.ifPresent(CountDownLatch::countDown);
            return resources;
        }

        @Override
        public InputStream fetchExternalResource(final ExternalResourceDescriptor descriptor) {
            final boolean success = (fetchSuccessSequence.size() >= (fetchCounter.get() + 1))
                    ? fetchSuccessSequence.get(fetchCounter.get())
                    : fetchSuccessSequence.get(fetchSuccessSequence.size() - 1);

            fetchCounter.incrementAndGet();
            countDownLatch.ifPresent(CountDownLatch::countDown);

            if (success) {
                return new ByteArrayInputStream(CONTENT_PROVIDED.getBytes(StandardCharsets.UTF_8));
            } else {
                throw new RuntimeException();
            }
        }

        public int getListCounter() {
            return listCounter.get();
        }

        public int getFetchCounter() {
            return fetchCounter.get();
        }
    }
}
