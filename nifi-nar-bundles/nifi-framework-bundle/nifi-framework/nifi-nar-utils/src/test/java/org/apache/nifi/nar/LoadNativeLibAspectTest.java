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
package org.apache.nifi.nar;

import org.aspectj.lang.ProceedingJoinPoint;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LoadNativeLibAspectTest {

    private static final Path TEMP_DIR = Paths.get(System.getProperty("java.io.tmpdir"));

    private LoadNativeLibAspect aspect;

    private ProceedingJoinPoint joinPoint;

    @Before
    public void setUp() {
        aspect = new LoadNativeLibAspect();
        joinPoint = mock(ProceedingJoinPoint.class);
    }

    @Test
    public void testWhenArgumentIsNullThenProceedNormally() throws Throwable {
        // GIVEN
        when(joinPoint.getArgs()).thenReturn(new Object[]{null});

        // WHEN
        aspect.around(joinPoint);

        // THEN
        verify(joinPoint).proceed();
    }

    @Test
    public void testWhenArgumentIsEmptyStringThenProceedNormally() throws Throwable {
        // GIVEN
        when(joinPoint.getArgs()).thenReturn(new Object[]{""});

        // WHEN
        aspect.around(joinPoint);

        // THEN
        verify(joinPoint).proceed();
    }

    @Test
    public void testWhenNativeLibraryFileNotExistsThenProceedNormally() throws Throwable {
        // GIVEN
        String libFileName = "mylib_dummy.so";
        Path libFilePath = Paths.get("target", libFileName).toAbsolutePath();

        when(joinPoint.getArgs()).thenReturn(new Object[]{libFilePath.toString()});

        // WHEN
        aspect.around(joinPoint);

        // THEN
        verify(joinPoint).proceed();
    }

    @Test
    public void testWhenNativeLibraryFileExistsThenCreateATempCopyAndProceedWithThat() throws Throwable {
        // GIVEN
        String libFileName = "mylib.so";
        byte[] libFileContent = "code".getBytes();
        Path libFilePath = Paths.get("target", libFileName).toAbsolutePath();
        Files.write(libFilePath, libFileContent);

        when(joinPoint.getArgs()).thenReturn(new Object[]{libFilePath.toString()});

        // WHEN
        aspect.around(joinPoint);

        // THEN
        ArgumentCaptor<Object[]> captor = ArgumentCaptor.forClass(Object[].class);
        verify(joinPoint).proceed(captor.capture());

        Object[] args = captor.getValue();
        assertNotNull(args);
        assertEquals(1, args.length);
        assertNotNull(args[0]);
        assertTrue(args[0] instanceof String);

        String tempLibFilePathStr = (String) args[0];
        Path tempLibFilePath = Paths.get(tempLibFilePathStr);
        assertEquals(TEMP_DIR, tempLibFilePath.getParent());
        assertTrue(tempLibFilePathStr.endsWith(libFileName));
        assertTrue(Files.exists(tempLibFilePath));
        assertArrayEquals(libFileContent, Files.readAllBytes(tempLibFilePath));

        Files.delete(libFilePath);
        Files.delete(tempLibFilePath);
    }

}
