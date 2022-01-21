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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

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


@RunWith(MockitoJUnitRunner.class)
public class BufferingExternalResourceProviderWorkerTest {
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
    private BufferingExternalResourceProviderWorker testSubject;

    @Before
    public void setUp() throws IOException {
        Mockito.when(conflictResolutionStrategy.shouldBeFetched(Mockito.any(File.class), Mockito.any(ExternalResourceDescriptor.class))).thenReturn(true);

        if (TARGET_DIRECTORY.exists()) {
            FileUtils.deleteFile(TARGET_DIRECTORY, true);
        }
    }

    @AfterClass
    public static void tearDown() throws IOException {
        if (TARGET_DIRECTORY.exists()) {
            FileUtils.deleteFile(TARGET_DIRECTORY, true);
        }
    }

    @Test
    public void testHappyPathWhenNoResourcesFound() throws InterruptedException {
        // given
        givenActions(Action.LIST, Action.LIST, Action.LIST);
        givenProviderWithoutResource();
        givenTestSubject();

        // when
        whenRunningTestSubject();

        // then
        thenAssertOperationsAreFinished();
        thenConflictResulotionWasNotNeeded();
        thenTargetFolderIsEmpty();
    }

    @Test
    public void testRunWithPreExistingDirectory() throws InterruptedException, IOException {
        // given
        givenActions(Action.LIST);
        givenTargetDirectoryExists(true);
        givenProviderWithoutResource();
        givenTestSubject();

        // when
        whenRunningTestSubject();

        // then
        thenAssertOperationsAreFinished();
        thenTargetFolderIsEmpty();
    }

    @Test
    public void testRunWithPreExistingDirectoryWhenMissingPrivileges() throws InterruptedException, IOException {
        // given
        givenNoAction();
        givenTargetDirectoryExists(false);
        givenProviderWithoutResource();
        givenTestSubject();

        // when
        whenRunningTestSubject();

        // then
        thenAssertOperationsAreFinished();
    }

    @Test
    public void testHappyPathWhenThereAreResources() throws InterruptedException, IOException {
        // given
        givenActions(Action.LIST, Action.FETCH, Action.FETCH);
        final ExternalResourceDescriptor resource1 = givenAvailableResource(RESOURCE_NAME_1);
        final ExternalResourceDescriptor resource2 = givenAvailableResource(RESOURCE_NAME_2);
        givenProviderWithResource(resource1, resource2);
        givenTestSubject();

        // when
        whenRunningTestSubject();

        // then
        thenAssertOperationsAreFinished();
        thenConflictResulotionWasNeeded(resource1, 1);
        thenConflictResulotionWasNeeded(resource2, 1);
        thenTargetFolderContains(CONTENT_PROVIDED, resource1, resource2);
    }

    @Test
    public void testWhenSomeResourcesAreConflicting() throws InterruptedException, IOException {
        // given
        givenActions(Action.LIST, Action.FETCH);
        final ExternalResourceDescriptor resource1 = givenAvailableResource(RESOURCE_NAME_1);
        final ExternalResourceDescriptor resource2 = givenAvailableResource(RESOURCE_NAME_2);
        givenProviderWithResource(resource1, resource2);
        givenResourceShouldNotBeFetched(resource1);
        givenTestSubject();

        // when
        whenRunningTestSubject();

        // then
        thenWaitForFileOperationsToFinish();
        thenConflictResulotionWasNeeded(resource1, 1);
        thenConflictResulotionWasNeeded(resource2, 1);
        thenTargetFolderContains(CONTENT_PROVIDED, resource2);
    }

    @Test
    public void testRetryAfterFailure() throws InterruptedException, IOException {
        // given
        givenActions(Action.LIST, Action.FETCH, Action.LIST, Action.FETCH);
        final ExternalResourceDescriptor resource1 = givenAvailableResource(RESOURCE_NAME_1);
        givenProviderWithResourceAndFetchSuccessSequence(resource1, false, true);
        givenTestSubject();

        // when
        whenRunningTestSubject();

        // then
        thenWaitForFileOperationsToFinish();
        thenConflictResulotionWasNeeded(resource1, 2);
        thenTargetFolderContains(CONTENT_PROVIDED, resource1);
    }

    @Test
    public void testExistingFileIsReplaced() throws InterruptedException, IOException {
        // given
        givenActions(Action.LIST, Action.FETCH);
        givenTargetDirectoryExists(true);
        givenFileExists(RESOURCE_NAME_1, CONTENT_LOCAL,true);
        final ExternalResourceDescriptor resource = givenAvailableResource(RESOURCE_NAME_1);
        givenProviderWithResource(resource);
        givenTestSubject();

        // when
        whenRunningTestSubject();

        // then
        thenWaitForFileOperationsToFinish();
        thenTargetFolderContains(CONTENT_PROVIDED, resource);
    }

    @Test
    @Ignore("This test needs human interaction and should not part of a build")
    public void testExistingFileCannotBeDeleted() throws InterruptedException, IOException {
        // given
        givenActions(Action.LIST, Action.FETCH);
        givenTargetDirectoryExists(true);
        givenFileExists(RESOURCE_NAME_1, CONTENT_LOCAL, false);
        final ExternalResourceDescriptor resource = givenAvailableResource(RESOURCE_NAME_1);
        givenProviderWithResource(resource);
        givenTestSubject();

        // when
        whenRunningTestSubject();

        // then
        thenWaitForFileOperationsToFinish();
        thenTargetFolderContains(CONTENT_LOCAL, resource);
        thenGivenPathIsAccessible(RESOURCE_NAME_1);
    }

    @Test
    @Ignore("This test needs human interaction and should not part of a build")
    public void testExistingTemporaryFileCannotBeDeleted() throws InterruptedException, IOException {
        // given
        givenActions(Action.LIST, Action.FETCH);
        givenTargetDirectoryExists(true);
        final ExternalResourceDescriptor resource = givenAvailableResource(RESOURCE_NAME_1);
        givenProviderWithResource(resource);
        givenTestSubject();
        final String tempFileName = givenTempFileName();
        givenFileExists(tempFileName, CONTENT_TEMP,false);

        // when
        whenRunningTestSubject();

        // then
        thenWaitForFileOperationsToFinish();
        thenConflictResulotionWasNeeded(resource, 1);
        thenTargetFolderContains(CONTENT_TEMP, tempFileName);
        thenGivenPathIsAccessible(tempFileName);
    }

    private void givenNoAction() {
        givenActions();
    }

    private void givenActions(Action... actions) {
        this.actions = new Actions(actions);
    }

    private void givenTargetDirectoryExists(boolean accessible) throws IOException {
        FileUtils.ensureDirectoryExistAndCanReadAndWrite(TARGET_DIRECTORY);

        if (!accessible) {
            TARGET_DIRECTORY.setWritable(false);
        }
    }

    private ExternalResourceDescriptor givenAvailableResource(String resourceName) {
        final ExternalResourceDescriptor resource = new ImmutableExternalResourceDescriptor(resourceName, System.currentTimeMillis());
        return resource;
    }

    private void givenTestSubject() {
        testSubject = new BufferingExternalResourceProviderWorker(PREFIX, this.getClass().getClassLoader(), provider, conflictResolutionStrategy, TARGET_DIRECTORY, 50, new CountDownLatch(0));
    }

    private void givenProviderWithResourceAndFetchSuccessSequence(final ExternalResourceDescriptor resource, final Boolean... fetchSuccessSequence) {
        countDownLatch = new CountDownLatch(actions.numberOfAllActions());
        provider = new TestExternalResourceProvider(Collections.singletonList(resource), countDownLatch, Arrays.asList(fetchSuccessSequence));
    }

    private void givenProviderWithResource(final ExternalResourceDescriptor... resources) {
        countDownLatch = new CountDownLatch(actions.numberOfAllActions());
        provider = new TestExternalResourceProvider(Arrays.asList(resources), countDownLatch);
    }

    private void givenProviderWithoutResource() {
        givenProviderWithResource();
    }

    private void givenFileExists(final String fileName, final String content, final boolean accessible) throws IOException {
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

    private String givenTempFileName() {
        return ".provider_" + testSubject.getName().substring(PREFIX.length() + 3) + ".tmp";
    }

    private void givenResourceShouldNotBeFetched(final ExternalResourceDescriptor resource) {
        Mockito.when(conflictResolutionStrategy.shouldBeFetched(TARGET_DIRECTORY, resource)).thenReturn(false);
    }

    private void whenRunningTestSubject() throws InterruptedException {
        final Thread workerThread = new Thread(testSubject);
        workerThread.start();
        countDownLatch.await();
        testSubject.stop();
    }

    private void thenAssertFileContains(final File file, final String expectedContent) throws IOException {
        try (
            final FileReader fileReader = new FileReader(file);
            final BufferedReader bufferedReader = new BufferedReader(fileReader)
        ) {
            final StringBuilder result = new StringBuilder();
            String line = "";

            while ((line = bufferedReader.readLine()) != null) {
                result.append(line);
            }

            Assert.assertEquals(expectedContent, result.toString());
        }
    }

    private void thenAssertOperationsAreFinished() throws InterruptedException {
        thenWaitForFileOperationsToFinish();
        Assert.assertFalse(testSubject.isRunning());
        Assert.assertEquals(actions.numberOfListings(), provider.getListCounter());
        Assert.assertEquals(actions.numberOfFetches(), provider.getFetchCounter());
    }

    private void thenConflictResulotionWasNotNeeded() {
        Mockito.verify(conflictResolutionStrategy, Mockito.never()).shouldBeFetched(Mockito.any(File.class), Mockito.any(ExternalResourceDescriptor.class));
    }

    private void thenConflictResulotionWasNeeded(final ExternalResourceDescriptor resource, final int times) {
        Mockito.verify(conflictResolutionStrategy, Mockito.times(times)).shouldBeFetched(TARGET_DIRECTORY, resource);
    }

    private void thenTargetFolderIsEmpty() {
        Assert.assertEquals(0, TARGET_DIRECTORY.list().length);
    }

    private void thenWaitForFileOperationsToFinish() throws InterruptedException {
        Thread.sleep(150);
    }

    private void thenTargetFolderContains(final String content, final ExternalResourceDescriptor... resources) throws IOException {
        Assert.assertEquals(resources.length, TARGET_DIRECTORY.list().length);

        for (final ExternalResourceDescriptor resource : resources) {
            File acquiredResource = new File(TARGET_DIRECTORY_PATH, resource.getLocation());
            Assert.assertTrue(acquiredResource.exists());
            thenAssertFileContains(acquiredResource, content);
        }
    }

    private void thenTargetFolderContains(final String content, final ExternalResourceDescriptor resource) throws IOException {
        thenTargetFolderContains(content, resource.getLocation());
    }

    private void thenTargetFolderContains(final String content, final String fileName) throws IOException {
        Assert.assertEquals(1, TARGET_DIRECTORY.list().length);
        File acquiredResource = new File(TARGET_DIRECTORY_PATH, fileName);
        Assert.assertTrue(acquiredResource.exists());
        thenAssertFileContains(acquiredResource, content);
    }

    private void thenGivenPathIsAccessible(final String path) {
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
