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
package org.apache.nifi.util.file.classloader;

import org.junit.jupiter.api.Test;

import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestClassLoaderUtils {

    @Test
    public void testGetCustomClassLoader() throws MalformedURLException, ClassNotFoundException {
        final String jarFilePath = "src/test/resources/TestClassLoaderUtils";
        ClassLoader customClassLoader =  ClassLoaderUtils.getCustomClassLoader(jarFilePath, this.getClass().getClassLoader(), getJarFilenameFilter());
        assertNotNull(customClassLoader);
        assertNotNull(customClassLoader.loadClass("TestSuccess"));
    }

    @Test
    public void testGetCustomClassLoaderNoPathSpecified() throws MalformedURLException {
        final ClassLoader originalClassLoader = this.getClass().getClassLoader();
        ClassLoader customClassLoader =  ClassLoaderUtils.getCustomClassLoader(null, originalClassLoader, getJarFilenameFilter());
        assertNotNull(customClassLoader);
        ClassNotFoundException cex = assertThrows(ClassNotFoundException.class, () -> customClassLoader.loadClass("TestSuccess"));
        assertEquals("TestSuccess", cex.getLocalizedMessage());
    }

    @Test
    public void testGetCustomClassLoaderWithInvalidPath() {
        final String jarFilePath = "src/test/resources/FakeTestClassLoaderUtils/TestSuccess.jar";
        MalformedURLException mex = assertThrows(MalformedURLException.class,
                () -> ClassLoaderUtils.getCustomClassLoader(jarFilePath, this.getClass().getClassLoader(), getJarFilenameFilter()));
        assertEquals("Path specified does not exist", mex.getLocalizedMessage());
    }

    @Test
    public void testGetCustomClassLoaderWithMultipleLocations() throws Exception {
        final String jarFilePath = " src/test/resources/TestClassLoaderUtils/TestSuccess.jar, http://nifi.apache.org/Test.jar";
        assertNotNull(ClassLoaderUtils.getCustomClassLoader(jarFilePath, this.getClass().getClassLoader(), getJarFilenameFilter()));
    }

    @Test
    public void testGetCustomClassLoaderWithEmptyLocations() throws Exception {
        String jarFilePath = "";
        assertNotNull(ClassLoaderUtils.getCustomClassLoader(jarFilePath, this.getClass().getClassLoader(), getJarFilenameFilter()));

        jarFilePath = ",";
        assertNotNull(ClassLoaderUtils.getCustomClassLoader(jarFilePath, this.getClass().getClassLoader(), getJarFilenameFilter()));

        jarFilePath = ",src/test/resources/TestClassLoaderUtils/TestSuccess.jar, ";
        assertNotNull(ClassLoaderUtils.getCustomClassLoader(jarFilePath, this.getClass().getClassLoader(), getJarFilenameFilter()));
    }

    @Test
    public void testGetURLsForClasspathWithDirectory() throws MalformedURLException {
        final String jarFilePath = "src/test/resources/TestClassLoaderUtils";
        URL[] urls = ClassLoaderUtils.getURLsForClasspath(jarFilePath, getJarFilenameFilter(), false);
        assertEquals(2, urls.length);
    }

    @Test
    public void testGetURLsForClasspathWithSingleJAR() throws MalformedURLException {
        final String jarFilePath = "src/test/resources/TestClassLoaderUtils/TestSuccess.jar";
        URL[] urls = ClassLoaderUtils.getURLsForClasspath(jarFilePath, null, false);
        assertEquals(1, urls.length);
    }

    @Test
    public void testGetURLsForClasspathWithSomeNonExistentAndNoSuppression() throws MalformedURLException {
        final String jarFilePath = "src/test/resources/TestClassLoaderUtils/TestSuccess.jar,src/test/resources/TestClassLoaderUtils/FakeTest.jar";
        assertThrows(MalformedURLException.class, () -> ClassLoaderUtils.getURLsForClasspath(jarFilePath, null, false));
    }

    @Test
    public void testGetURLsForClasspathWithSomeNonExistentAndSuppression() throws MalformedURLException {
        final String jarFilePath = "src/test/resources/TestClassLoaderUtils/TestSuccess.jar,src/test/resources/TestClassLoaderUtils/FakeTest.jar";
        URL[] urls = ClassLoaderUtils.getURLsForClasspath(jarFilePath, null, true);
        assertEquals(1, urls.length);
    }

    @Test
    public void testGetURLsForClasspathWithSetAndSomeNonExistentAndSuppression() throws MalformedURLException {
        final Set<String> modules = new HashSet<>();
        modules.add("src/test/resources/TestClassLoaderUtils/TestSuccess.jar,src/test/resources/TestClassLoaderUtils/FakeTest1.jar");
        modules.add("src/test/resources/TestClassLoaderUtils/FakeTest2.jar,src/test/resources/TestClassLoaderUtils/FakeTest3.jar");

        URL[] urls = ClassLoaderUtils.getURLsForClasspath(modules, null, true);
        assertEquals(1, urls.length);
    }

    @Test
    public void testGenerateAdditionalUrlsFingerprintForFileUrl() throws MalformedURLException {
        final Set<URL> urls = new HashSet<>();
        URL testUrl = Paths.get("src/test/resources/TestClassLoaderUtils/TestSuccess.jar").toUri().toURL();
        urls.add(testUrl);
        String testFingerprint = ClassLoaderUtils.generateAdditionalUrlsFingerprint(urls, null);
        assertNotNull(testFingerprint);
    }

    @Test
    public void testGenerateAdditionalUrlsFingerprintForHttpUrl() throws MalformedURLException {
        final Set<URL> urls = new HashSet<>();
        URL testUrl = URI.create("http://myhost/TestSuccess.jar").toURL();
        urls.add(testUrl);
        String testFingerprint = ClassLoaderUtils.generateAdditionalUrlsFingerprint(urls, null);
        assertNotNull(testFingerprint);
    }

    protected FilenameFilter getJarFilenameFilter() {
        return  (dir, name) -> name != null && name.endsWith(".jar");
    }
}
