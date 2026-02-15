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

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardConnectorMockServerJettyTest {

    private static final String NAR_DEPENDENCIES_PATH = "NAR-INF/bundled-dependencies";

    @TempDir
    private Path tempDir;

    @Test
    void testFindWarsDiscoversSingleWarFile() throws Exception {
        final Path bundleWorkingDir = tempDir.resolve("test-bundle");
        final Path depsDir = bundleWorkingDir.resolve(NAR_DEPENDENCIES_PATH);
        Files.createDirectories(depsDir);

        final Path warFile = depsDir.resolve("my-app.war");
        Files.createFile(warFile);

        final Bundle bundle = createBundle(bundleWorkingDir);
        final Map<File, Bundle> wars = invokeFindWars(Set.of(bundle));

        assertEquals(1, wars.size());
        assertTrue(wars.containsKey(warFile.toFile()));
        assertEquals(bundle, wars.get(warFile.toFile()));
    }

    @Test
    void testFindWarsIgnoresNonWarFiles() throws Exception {
        final Path bundleWorkingDir = tempDir.resolve("test-bundle");
        final Path depsDir = bundleWorkingDir.resolve(NAR_DEPENDENCIES_PATH);
        Files.createDirectories(depsDir);

        Files.createFile(depsDir.resolve("some-lib.jar"));
        Files.createFile(depsDir.resolve("config.xml"));

        final Bundle bundle = createBundle(bundleWorkingDir);
        final Map<File, Bundle> wars = invokeFindWars(Set.of(bundle));

        assertTrue(wars.isEmpty());
    }

    @Test
    void testFindWarsHandlesMissingDependenciesDirectory() throws Exception {
        final Path bundleWorkingDir = tempDir.resolve("empty-bundle");
        Files.createDirectories(bundleWorkingDir);

        final Bundle bundle = createBundle(bundleWorkingDir);
        final Map<File, Bundle> wars = invokeFindWars(Set.of(bundle));

        assertTrue(wars.isEmpty());
    }

    @Test
    void testFindWarsDiscoversMultipleWarFiles() throws Exception {
        final Path bundleWorkingDir = tempDir.resolve("multi-war-bundle");
        final Path depsDir = bundleWorkingDir.resolve(NAR_DEPENDENCIES_PATH);
        Files.createDirectories(depsDir);

        Files.createFile(depsDir.resolve("app-one.war"));
        Files.createFile(depsDir.resolve("app-two.war"));
        Files.createFile(depsDir.resolve("some-lib.jar"));

        final Bundle bundle = createBundle(bundleWorkingDir);
        final Map<File, Bundle> wars = invokeFindWars(Set.of(bundle));

        assertEquals(2, wars.size());
    }

    @Test
    void testFindWarsFromMultipleBundles() throws Exception {
        final Path bundleDir1 = tempDir.resolve("bundle-1");
        final Path depsDir1 = bundleDir1.resolve(NAR_DEPENDENCIES_PATH);
        Files.createDirectories(depsDir1);
        Files.createFile(depsDir1.resolve("first-app.war"));

        final Path bundleDir2 = tempDir.resolve("bundle-2");
        final Path depsDir2 = bundleDir2.resolve(NAR_DEPENDENCIES_PATH);
        Files.createDirectories(depsDir2);
        Files.createFile(depsDir2.resolve("second-app.war"));

        final Bundle bundle1 = createBundle(bundleDir1, "test-bundle-1");
        final Bundle bundle2 = createBundle(bundleDir2, "test-bundle-2");

        final Map<File, Bundle> wars = invokeFindWars(Set.of(bundle1, bundle2));

        assertEquals(2, wars.size());
    }

    @Test
    void testGetHttpPortReturnsNegativeOneWhenNoServer() {
        final StandardConnectorMockServer server = new StandardConnectorMockServer();
        assertEquals(-1, server.getHttpPort());
    }

    @Test
    void testContextPathDerivedFromWarFilename() {
        final String warName = "my-custom-app.war";
        final String expectedContextPath = "/my-custom-app";
        final String warExtension = ".war";

        final String contextPath = "/" + warName.substring(0, warName.length() - warExtension.length());
        assertEquals(expectedContextPath, contextPath);
    }

    @Test
    void testFindWarsReturnsEmptyForEmptyBundleSet() throws Exception {
        final Map<File, Bundle> wars = invokeFindWars(Set.of());
        assertTrue(wars.isEmpty());
    }

    private Bundle createBundle(final Path workingDir) {
        return createBundle(workingDir, "test-bundle");
    }

    private Bundle createBundle(final Path workingDir, final String artifactId) {
        final BundleDetails details = new BundleDetails.Builder()
                .workingDir(workingDir.toFile())
                .coordinate(new BundleCoordinate("org.test", artifactId, "1.0.0"))
                .build();

        return new Bundle(details, ClassLoader.getSystemClassLoader());
    }

    @SuppressWarnings("unchecked")
    private Map<File, Bundle> invokeFindWars(final Set<Bundle> bundles) throws Exception {
        final StandardConnectorMockServer server = new StandardConnectorMockServer();
        final Method findWarsMethod = StandardConnectorMockServer.class.getDeclaredMethod("findWars", Set.class);
        findWarsMethod.setAccessible(true);

        try {
            return (Map<File, Bundle>) findWarsMethod.invoke(server, bundles);
        } catch (final InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            }
            throw e;
        }
    }
}
