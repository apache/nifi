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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AbstractNativeLibHandlingClassLoaderTest {
    public static final String NATIVE_LIB_NAME = "native_lib";

    @Mock
    private AbstractNativeLibHandlingClassLoader testSubjectHelper;

    @TempDir
    private Path tempDirectory;

    private String javaLibraryPath = "";

    private List<File> nativeLibDirs = new ArrayList<>();
    private final Map<String, Path> nativeLibNameToPath = new HashMap<>();

    private boolean isOsWindows;
    private boolean isOsMaxOsx;
    private boolean isOsLinux;

    @Test
    public void testFindLibraryShouldReturnNullOnWindowsWhenNoDLLAvailable() throws Exception {
        isOsWindows = true;

        createTempFile("so");
        createTempFile("lib", "so");
        createTempFile("dylib");
        createTempFile("lib", "dylib");

        String expected = null;

        testFindLibrary(expected);
    }

    @Test
    public void testFindLibraryShouldReturnDLLOnWindows() throws Exception {
        isOsWindows = true;

        Path expectedNativeLib = createTempFile("dll");
        createTempFile("so");
        createTempFile("lib", "so");
        createTempFile("dylib");
        createTempFile("lib", "dylib");

        String expected = expectedNativeLib.toFile().getAbsolutePath();

        testFindLibrary(expected);
    }

    @Test
    public void testFindLibraryShouldReturnNullOnMacWhenNoDylibOrSoAvailable() throws Exception {
        isOsMaxOsx = true;

        createTempFile("dll");

        String expected = null;

        testFindLibrary(expected);
    }

    @Test
    public void testFindLibraryShouldReturnDylibOnMac() throws Exception {
        isOsMaxOsx = true;

        createTempFile("dll");
        createTempFile("so");
        createTempFile("lib", "so");
        Path expectedNativeLib = createTempFile("dylib");
        createTempFile("lib", "dylib");

        String expected = expectedNativeLib.toFile().getAbsolutePath();

        testFindLibrary(expected);
    }

    @Test
    public void testFindLibraryShouldReturnLibDylibOnMac() throws Exception {
        isOsMaxOsx = true;

        createTempFile("dll");
        createTempFile("so");
        createTempFile("lib", "so");
        Path expectedNativeLib = createTempFile("lib", "dylib");

        String expected = expectedNativeLib.toFile().getAbsolutePath();

        testFindLibrary(expected);
    }

    @Test
    public void testFindLibraryMayReturnSoOnMac() throws Exception {
        isOsMaxOsx = true;

        createTempFile("dll");
        Path expectedNativeLib = createTempFile("so");
        createTempFile("lib", "so");

        String expected = expectedNativeLib.toFile().getAbsolutePath();

        testFindLibrary(expected);
    }

    @Test
    public void testFindLibraryMayReturnLibSoOnMac() throws Exception {
        isOsMaxOsx = true;

        createTempFile("dll");
        Path expectedNativeLib = createTempFile("lib", "so");

        String expected = expectedNativeLib.toFile().getAbsolutePath();

        testFindLibrary(expected);
    }

    @Test
    public void testFindLibraryShouldReturnNullOnLinuxWhenNoSoAvailable() throws Exception {
        isOsLinux = true;

        createTempFile("dll");
        createTempFile("dylib");
        createTempFile("lib", "dylib");

        String expected = null;

        testFindLibrary(expected);
    }

    @Test
    public void testFindLibraryShouldReturnSoOnLinux() throws Exception {
        isOsLinux = true;

        createTempFile("dll");
        Path expectedNativeLib = createTempFile("so");
        createTempFile("lib", "so");
        createTempFile("dylib");
        createTempFile("lib", "dylib");

        String expected = expectedNativeLib.toFile().getAbsolutePath();

        testFindLibrary(expected);
    }

    @Test
    public void testFindLibraryShouldReturnLibSoOnLinux() throws Exception {
        isOsLinux = true;

        createTempFile("dll");
        Path expectedNativeLib = createTempFile("lib", "so");
        createTempFile("dylib");
        createTempFile("lib", "dylib");

        String expected = expectedNativeLib.toFile().getAbsolutePath();

        testFindLibrary(expected);
    }

    private void testFindLibrary(String expected) {
        String actual = createTestSubjectForOS().findLibrary(NATIVE_LIB_NAME, tempDirectory.toFile());

        assertEquals(expected, actual);
    }

    @Test
    public void testFindLibraryShouldReturnLibLocation() throws Exception {
        File nativeLibDir = mock(File.class);

        nativeLibDirs = Collections.singletonList(nativeLibDir);

        Path libPath = createTempFile("mocked").toAbsolutePath();
        when(testSubjectHelper.findLibrary("libName", nativeLibDir)).thenReturn("libLocation");
        when(testSubjectHelper.createTempCopy("libName", "libLocation")).thenReturn(libPath);

        String expected = libPath.toFile().getAbsolutePath();

        AbstractNativeLibHandlingClassLoader testSubject = createTestSubject();

        String actual = testSubject.findLibrary("libName");

        assertEquals(expected, actual);
        verify(testSubjectHelper).findLibrary("libName", nativeLibDir);
        verify(testSubjectHelper).createTempCopy("libName", "libLocation");
        verifyNoMoreInteractions(testSubjectHelper);
    }

    @Test
    public void testFindLibraryShouldReturnFirstFoundLibLocation() throws Exception {
        File nativeLibDir1 = mock(File.class);
        File nativeLibDir2 = mock(File.class);
        File nativeLibDir3 = mock(File.class);

        nativeLibDirs = Arrays.asList(nativeLibDir1, nativeLibDir2, nativeLibDir3);

        Path libPath = createTempFile("mocked").toAbsolutePath();
        when(testSubjectHelper.findLibrary("libName", nativeLibDir1)).thenReturn(null);
        when(testSubjectHelper.findLibrary("libName", nativeLibDir2)).thenReturn("firstFoundLibLocation");
        when(testSubjectHelper.createTempCopy("libName", "firstFoundLibLocation")).thenReturn(libPath);

        String expected = libPath.toFile().getAbsolutePath();

        AbstractNativeLibHandlingClassLoader testSubject = createTestSubject();

        String actual = testSubject.findLibrary("libName");

        assertEquals(expected, actual);
        verify(testSubjectHelper).findLibrary("libName", nativeLibDir1);
        verify(testSubjectHelper).findLibrary("libName", nativeLibDir2);
        verify(testSubjectHelper).createTempCopy("libName", "firstFoundLibLocation");
        verifyNoMoreInteractions(testSubjectHelper);
    }

    @Test
    public void testFindLibraryShouldReturnCachedLibLocation() throws Exception {
        File nativeLibDir = mock(File.class);

        nativeLibDirs = Collections.singletonList(nativeLibDir);

        Path cachedLibPath = createTempFile("cached", "mocked").toAbsolutePath();
        nativeLibNameToPath.put("libName", cachedLibPath);

        AbstractNativeLibHandlingClassLoader testSubject = createTestSubject();
        String expected = cachedLibPath.toFile().getAbsolutePath();

        String actual = testSubject.findLibrary("libName");

        assertEquals(expected, actual);
        verifyNoMoreInteractions(testSubjectHelper);
    }

    @Test
    public void testFindLibraryShouldReturnFoundThenCachedLibLocation() throws Exception {
        File nativeLibDir = mock(File.class);

        nativeLibDirs = Collections.singletonList(nativeLibDir);

        Path libPath = createTempFile("mocked").toAbsolutePath();
        when(testSubjectHelper.findLibrary("libName", nativeLibDir)).thenReturn("libLocation");
        when(testSubjectHelper.createTempCopy("libName", "libLocation")).thenReturn(libPath);

        String expected = libPath.toFile().getAbsolutePath();

        AbstractNativeLibHandlingClassLoader testSubject = createTestSubject();

        String actual1 = testSubject.findLibrary("libName");
        String actual2 = testSubject.findLibrary("libName");

        assertEquals(expected, actual1);
        assertEquals(expected, actual2);
        verify(testSubjectHelper).findLibrary("libName", nativeLibDir);
        verify(testSubjectHelper).createTempCopy("libName", "libLocation");
        verifyNoMoreInteractions(testSubjectHelper);
    }

    @Test
    public void testFindLibraryShouldReturnNullWhenLibDirNotRegistered() {
        nativeLibDirs = new ArrayList<>();

        AbstractNativeLibHandlingClassLoader testSubject = createTestSubject();
        String expected = null;

        String actual = testSubject.findLibrary("libName");

        assertEquals(expected, actual);
        verifyNoMoreInteractions(testSubjectHelper);
    }

    @Test
    public void testFindLibraryShouldReturnNullWhenLibNotFound() {
        File nativeLibDir = mock(File.class);

        nativeLibDirs = Collections.singletonList(nativeLibDir);

        when(testSubjectHelper.findLibrary("libName", nativeLibDir)).thenReturn(null);

        AbstractNativeLibHandlingClassLoader testSubject = createTestSubject();
        String expected = null;

        String actual = testSubject.findLibrary("libName");

        assertEquals(expected, actual);
        verify(testSubjectHelper).findLibrary("libName", nativeLibDir);
        verifyNoMoreInteractions(testSubjectHelper);
    }

    @Test
    public void testToDirShouldReturnNullForNullInput() {
        File expected = null;

        File actual = createTestSubject().toDir(null);

        assertEquals(expected, actual);
    }

    @Test
    public void testToDirShouldReturnParentForFile() throws Exception {
        Path filePath = createTempFile("mocked").toAbsolutePath();
        File expected = filePath.getParent().toFile();

        File actual = createTestSubject().toDir(filePath.toFile());

        assertEquals(expected, actual);
    }

    @Test
    public void testToDirShouldReturnDirUnchanged() throws Exception {
        Path dirPath = createTempFile("mocked").getParent();
        File expected = dirPath.toFile();

        File actual = createTestSubject().toDir(dirPath.toFile());

        assertEquals(expected, actual);
    }

    @Test
    public void testGetUsrLibDirsShouldReturnUniqueDirs() throws Exception {
        Path dir1 = Files.createDirectory(tempDirectory.resolve("dir1"));
        Path dir2 = Files.createDirectory(tempDirectory.resolve("dir2"));
        Path dir3 = Files.createDirectory(tempDirectory.resolve("dir3"));
        Path dir4 = Files.createDirectory(tempDirectory.resolve("dir4"));

        Path file11 = createTempFile(dir1, "usrLib", "file11");
        Path file12 = createTempFile(dir1, "usrLib", "file12");
        Path file21 = createTempFile(dir2, "usrLib", "file21");
        Path file31 = createTempFile(dir3, "usrLib", "file31");

        javaLibraryPath = Stream.of(
                file11,
                file12,
                file21,
                file31,
                dir3,
                dir4
        )
                .map(Path::toFile)
                .map(File::getAbsolutePath)
                .collect(Collectors.joining(File.pathSeparator));

        HashSet<File> expected = new HashSet<>();
        expected.add(dir1.toFile());
        expected.add(dir2.toFile());
        expected.add(dir3.toFile());
        expected.add(dir4.toFile());

        Set<File> actual = createTestSubject().getUsrLibDirs();

        assertEquals(expected, actual);
    }

    private AbstractNativeLibHandlingClassLoader createTestSubjectForOS() {
        AbstractNativeLibHandlingClassLoader testSubject = new AbstractNativeLibHandlingClassLoader(new URL[0], nativeLibDirs, "unimportant") {
            @Override
            public boolean isOsWindows() {
                return isOsWindows;
            }

            @Override
            public boolean isOsMac() {
                return isOsMaxOsx;
            }

            @Override
            public boolean isOsLinuxUnix() {
                return isOsLinux;
            }

            @Override
            public String getJavaLibraryPath() {
                return javaLibraryPath;
            }
        };

        return testSubject;
    }

    private AbstractNativeLibHandlingClassLoader createTestSubject() {
        AbstractNativeLibHandlingClassLoader testSubject = new AbstractNativeLibHandlingClassLoader(new URL[0], nativeLibDirs, "unimportant") {
            @Override
            public Path createTempCopy(String libname, String libraryOriginalPathString) {
                return testSubjectHelper.createTempCopy(libname, libraryOriginalPathString);
            }

            @Override
            public String findLibrary(String libname, File nativeLibDir) {
                return testSubjectHelper.findLibrary(libname, nativeLibDir);
            }

            @Override
            public boolean isOsWindows() {
                return isOsWindows;
            }

            @Override
            public boolean isOsMac() {
                return isOsMaxOsx;
            }

            @Override
            public boolean isOsLinuxUnix() {
                return isOsLinux;
            }

            @Override
            public String getJavaLibraryPath() {
                return javaLibraryPath;
            }
        };

        testSubject.nativeLibNameToPath.putAll(this.nativeLibNameToPath);

        return testSubject;
    }

    private Path createTempFile(String suffix) throws IOException {
        return createTempFile("", suffix);
    }

    private Path createTempFile(String prefix, String suffix) throws IOException {
        return createTempFile(tempDirectory, prefix, suffix);
    }

    private Path createTempFile(Path tempDirectory, String prefix, String suffix) throws IOException {
        return Files.createFile(tempDirectory.resolve(prefix + NATIVE_LIB_NAME + "." + suffix));
    }
}
