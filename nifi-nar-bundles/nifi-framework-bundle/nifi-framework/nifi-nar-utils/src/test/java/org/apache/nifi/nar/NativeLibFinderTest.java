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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class NativeLibFinderTest {
    public static final String NATIVE_LIB_NAME = "native_lib";

    @Mock
    private NativeLibFinder testSubjectHelper;

    private String javaLibraryPath;

    private Path tempDirectory;
    private List<File> nativeLibDirs;
    private Map<String, Path> nativeLibNameToPath;

    private boolean isOsWindows;
    private boolean isOsMaxOsx;
    private boolean isOsLinux;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        tempDirectory = Files.createTempDirectory(this.getClass().getSimpleName());
    }

    @After
    public void tearDown() throws Exception {
        tempDirectory.toFile().deleteOnExit();

        Files.walk(tempDirectory)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);

    }

    @Test
    public void testFindLibraryShouldReturnNullOnWindowsWhenNoDLLAvailable() throws Exception {
        // GIVEN
        isOsWindows = true;

        createTempFile("so");
        createTempFile("lib", "so");
        createTempFile("dylib");
        createTempFile("lib", "dylib");

        String expected = null;

        // WHEN
        // THEN
        testFindLibrary(expected);
    }

    @Test
    public void testFindLibraryShouldReturnDLLOnWindows() throws Exception {
        // GIVEN
        isOsWindows = true;

        Path expectedNativeLib = createTempFile("dll");
        createTempFile("so");
        createTempFile("lib", "so");
        createTempFile("dylib");
        createTempFile("lib", "dylib");

        String expected = expectedNativeLib.toFile().getAbsolutePath();

        // WHEN
        // THEN
        testFindLibrary(expected);
    }

    @Test
    public void testFindLibraryShouldReturnNullOnMacWhenNoDylibOrSoAvailable() throws Exception {
        // GIVEN
        isOsMaxOsx = true;

        createTempFile("dll");

        String expected = null;

        // WHEN
        // THEN
        testFindLibrary(expected);
    }

    @Test
    public void testFindLibraryShouldReturnDylibOnMac() throws Exception {
        // GIVEN
        isOsMaxOsx = true;

        createTempFile("dll");
        createTempFile("so");
        createTempFile("lib", "so");
        Path expectedNativeLib = createTempFile("dylib");
        createTempFile("lib", "dylib");

        String expected = expectedNativeLib.toFile().getAbsolutePath();

        // WHEN
        // THEN
        testFindLibrary(expected);
    }

    @Test
    public void testFindLibraryShouldReturnLibDylibOnMac() throws Exception {
        // GIVEN
        isOsMaxOsx = true;

        createTempFile("dll");
        createTempFile("so");
        createTempFile("lib", "so");
        Path expectedNativeLib = createTempFile("lib", "dylib");

        String expected = expectedNativeLib.toFile().getAbsolutePath();

        // WHEN
        // THEN
        testFindLibrary(expected);
    }

    @Test
    public void testFindLibraryMayReturnSoOnMac() throws Exception {
        // GIVEN
        isOsMaxOsx = true;
        NativeLibFinder testSubject = createTestSubject();

        createTempFile("dll");
        Path expectedNativeLib = createTempFile("so");
        createTempFile("lib", "so");

        String expected = expectedNativeLib.toFile().getAbsolutePath();

        // WHEN
        // THEN
        testFindLibrary(expected);
    }

    @Test
    public void testFindLibraryMayReturnLibSoOnMac() throws Exception {
        // GIVEN
        isOsMaxOsx = true;
        NativeLibFinder testSubject = createTestSubject();

        createTempFile("dll");
        Path expectedNativeLib = createTempFile("lib", "so");

        String expected = expectedNativeLib.toFile().getAbsolutePath();

        // WHEN
        // THEN
        testFindLibrary(expected);
    }

    @Test
    public void testFindLibraryShouldReturnNullOnLinuxWhenNoSoAvailable() throws Exception {
        // GIVEN
        isOsLinux = true;
        NativeLibFinder testSubject = createTestSubject();

        createTempFile("dll");
        createTempFile("dylib");
        createTempFile("lib", "dylib");

        String expected = null;

        // WHEN
        // THEN
        testFindLibrary(expected);
    }

    @Test
    public void testFindLibraryShouldReturnSoOnLinux() throws Exception {
        // GIVEN
        isOsLinux = true;
        NativeLibFinder testSubject = createTestSubject();

        createTempFile("dll");
        Path expectedNativeLib = createTempFile("so");
        createTempFile("lib", "so");
        createTempFile("dylib");
        createTempFile("lib", "dylib");

        String expected = expectedNativeLib.toFile().getAbsolutePath();

        // WHEN
        // THEN
        testFindLibrary(expected);
    }

    @Test
    public void testFindLibraryShouldReturnLibSoOnLinux() throws Exception {
        // GIVEN
        isOsLinux = true;
        NativeLibFinder testSubject = createTestSubject();

        createTempFile("dll");
        Path expectedNativeLib = createTempFile("lib", "so");
        createTempFile("dylib");
        createTempFile("lib", "dylib");

        String expected = expectedNativeLib.toFile().getAbsolutePath();

        // WHEN
        // THEN
        testFindLibrary(expected);
    }

    private void testFindLibrary(String expected) {
        String actual = createTestSubjectForOS().findLibrary(NATIVE_LIB_NAME, tempDirectory.toFile());

        assertEquals(expected, actual);
    }

    @Test
    public void testFindLibraryShouldReturnLibLocation() throws Exception {
        // GIVEN
        File nativeLibDir = mock(File.class);

        nativeLibDirs = Arrays.asList(nativeLibDir);

        nativeLibNameToPath = new HashMap<>();

        Path libPath = createTempFile("mocked").toAbsolutePath();
        when(testSubjectHelper.findLibrary("libName", nativeLibDir)).thenReturn("libLocation");
        when(testSubjectHelper.createTempCopy("libName", "libLocation")).thenReturn(libPath);

        String expected = libPath.toFile().getAbsolutePath();

        NativeLibFinder testSubject = createTestSubject();

        // WHEN
        String actual = testSubject.findLibrary("libName");

        // THEN
        assertEquals(expected, actual);
        verify(testSubjectHelper).findLibrary("libName", nativeLibDir);
        verify(testSubjectHelper).createTempCopy("libName", "libLocation");
        verifyNoMoreInteractions(testSubjectHelper);
    }

    @Test
    public void testFindLibraryShouldReturnFirstFoundLibLocation() throws Exception {
        // GIVEN
        File nativeLibDir1 = mock(File.class);
        File nativeLibDir2 = mock(File.class);
        File nativeLibDir3 = mock(File.class);

        nativeLibDirs = Arrays.asList(nativeLibDir1, nativeLibDir2, nativeLibDir3);

        nativeLibNameToPath = new HashMap<>();

        Path libPath = createTempFile("mocked").toAbsolutePath();
        when(testSubjectHelper.findLibrary("libName", nativeLibDir1)).thenReturn(null);
        when(testSubjectHelper.findLibrary("libName", nativeLibDir2)).thenReturn("firstFoundLibLocation");
        when(testSubjectHelper.createTempCopy("libName", "firstFoundLibLocation")).thenReturn(libPath);

        String expected = libPath.toFile().getAbsolutePath();

        NativeLibFinder testSubject = createTestSubject();

        // WHEN
        String actual = testSubject.findLibrary("libName");

        // THEN
        assertEquals(expected, actual);
        verify(testSubjectHelper).findLibrary("libName", nativeLibDir1);
        verify(testSubjectHelper).findLibrary("libName", nativeLibDir2);
        verify(testSubjectHelper).createTempCopy("libName", "firstFoundLibLocation");
        verifyNoMoreInteractions(testSubjectHelper);
    }

    @Test
    public void testFindLibraryShouldReturnCachedLibLocation() throws Exception {
        // GIVEN
        File nativeLibDir = mock(File.class);

        nativeLibDirs = Arrays.asList(nativeLibDir);

        Path cachedLibPath = createTempFile("cached", "mocked").toAbsolutePath();
        nativeLibNameToPath = new HashMap<String, Path>() {{
            put("libName", cachedLibPath);
        }};

        NativeLibFinder testSubject = createTestSubject();
        String expected = cachedLibPath.toFile().getAbsolutePath();

        // WHEN
        String actual = testSubject.findLibrary("libName");

        // THEN
        assertEquals(expected, actual);
        verifyNoMoreInteractions(testSubjectHelper);
    }

    @Test
    public void testFindLibraryShouldReturnFoundThenCachedLibLocation() throws Exception {
        // GIVEN
        File nativeLibDir = mock(File.class);

        nativeLibDirs = Arrays.asList(nativeLibDir);

        nativeLibNameToPath = new HashMap<>();

        Path libPath = createTempFile("mocked").toAbsolutePath();
        when(testSubjectHelper.findLibrary("libName", nativeLibDir)).thenReturn("libLocation");
        when(testSubjectHelper.createTempCopy("libName", "libLocation")).thenReturn(libPath);

        String expected = libPath.toFile().getAbsolutePath();

        NativeLibFinder testSubject = createTestSubject();

        // WHEN
        String actual1 = testSubject.findLibrary("libName");
        String actual2 = testSubject.findLibrary("libName");

        // THEN
        assertEquals(expected, actual1);
        assertEquals(expected, actual2);
        verify(testSubjectHelper).findLibrary("libName", nativeLibDir);
        verify(testSubjectHelper).createTempCopy("libName", "libLocation");
        verifyNoMoreInteractions(testSubjectHelper);
    }

    @Test
    public void testFindLibraryShouldReturnNullWhenLibDirNotRegistered() throws Exception {
        // GIVEN
        nativeLibDirs = new ArrayList<>();

        nativeLibNameToPath = new HashMap<>();

        NativeLibFinder testSubject = createTestSubject();
        String expected = null;

        // WHEN
        String actual = testSubject.findLibrary("libName");

        // THEN
        assertEquals(expected, actual);
        verifyNoMoreInteractions(testSubjectHelper);
    }

    @Test
    public void testFindLibraryShouldReturnNullWhenLibNotFound() throws Exception {
        // GIVEN
        File nativeLibDir = mock(File.class);

        nativeLibDirs = Arrays.asList(nativeLibDir);

        nativeLibNameToPath = new HashMap<>();

        when(testSubjectHelper.findLibrary("libName", nativeLibDir)).thenReturn(null);

        NativeLibFinder testSubject = createTestSubject();
        String expected = null;

        // WHEN
        String actual = testSubject.findLibrary("libName");

        // THEN
        assertEquals(expected, actual);
        verify(testSubjectHelper).findLibrary("libName", nativeLibDir);
        verifyNoMoreInteractions(testSubjectHelper);
    }

    @Test
    public void testToDirAppliedShouldReturnParentForFile() throws Exception {
        // GIVEN
        Path filePath = createTempFile("mocked").toAbsolutePath();
        File expected = filePath.getParent().toFile();

        // WHEN
        File actual = createTestSubject().toDir().apply(filePath.toFile());

        // THEN
        assertEquals(expected, actual);
    }

    @Test
    public void testToDirAppliedShouldReturnDirUnchanged() throws Exception {
        // GIVEN
        Path dirPath = createTempFile("mocked").getParent();
        File expected = dirPath.toFile();

        // WHEN
        File actual = createTestSubject().toDir().apply(dirPath.toFile());

        // THEN
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

        javaLibraryPath = new HashSet<Path>() {{
            add(file11);
            add(file12);
            add(file21);
            add(file31);
            add(dir3);
            add(dir4);
        }}.stream()
                .map(Path::toFile)
                .map(File::getAbsolutePath)
                .collect(Collectors.joining(File.pathSeparator));

        HashSet<File> expected = new HashSet<File>() {{
            add(dir1.toFile());
            add(dir2.toFile());
            add(dir3.toFile());
            add(dir4.toFile());
        }};

        Set<File> actual = createTestSubject().getUsrLibDirs();

        assertEquals(expected, actual);
    }

    private NativeLibFinder createTestSubjectForOS() {
        NativeLibFinder testSubject = new NativeLibFinder() {
            @Override
            public List<File> getNativeLibDirs() {
                return null;
            }

            @Override
            public Map<String, Path> getNativeLibNameToPath() {
                return null;
            }

            @Override
            public String getTmpLibFilePrefix() {
                return null;
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
        };

        return testSubject;
    }

    private NativeLibFinder createTestSubject() {
        NativeLibFinder testSubject = new NativeLibFinder() {
            @Override
            public List<File> getNativeLibDirs() {
                return nativeLibDirs;
            }

            @Override
            public Map<String, Path> getNativeLibNameToPath() {
                return nativeLibNameToPath;
            }

            @Override
            public String getTmpLibFilePrefix() {
                return "unimportant";
            }

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
