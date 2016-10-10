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

import java.io.FilenameFilter;
import java.net.MalformedURLException;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestClassLoaderUtils {

    @Test
    public void testGetCustomClassLoader() throws MalformedURLException,ClassNotFoundException{
        final String jarFilePath = "src/test/resources/TestClassLoaderUtils";
        ClassLoader customClassLoader =  ClassLoaderUtils.getCustomClassLoader(jarFilePath ,this.getClass().getClassLoader(), getJarFilenameFilter());
        assertTrue(customClassLoader != null);
        assertTrue(customClassLoader.loadClass("TestSuccess") != null);
    }

    @Test
    public void testGetCustomClassLoaderNoPathSpecified() throws MalformedURLException{
        final ClassLoader originalClassLoader = this.getClass().getClassLoader();
        ClassLoader customClassLoader =  ClassLoaderUtils.getCustomClassLoader(null,originalClassLoader, getJarFilenameFilter());
        assertTrue(customClassLoader != null);
        try{
            customClassLoader.loadClass("TestSuccess");
        }catch (ClassNotFoundException cex){
            assertTrue(cex.getLocalizedMessage().equals("TestSuccess"));
            return;
        }
        fail("exception did not occur, class should not be found");
    }

    @Test
    public void testGetCustomClassLoaderWithInvalidPath() {
        final String jarFilePath = "src/test/resources/FakeTestClassLoaderUtils/TestSuccess.jar";
        try {
            ClassLoaderUtils.getCustomClassLoader(jarFilePath, this.getClass().getClassLoader(), getJarFilenameFilter());
        }catch(MalformedURLException mex){
            assertTrue(mex.getLocalizedMessage().equals("Path specified does not exist"));
            return;
        }
        fail("exception did not occur, path should not exist");
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

    protected FilenameFilter getJarFilenameFilter(){
        return  (dir, name) -> name != null && name.endsWith(".jar");
    }
}
