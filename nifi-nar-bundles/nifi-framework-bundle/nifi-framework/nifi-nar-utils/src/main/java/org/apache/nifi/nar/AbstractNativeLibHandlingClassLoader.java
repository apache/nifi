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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * An extension of {@link URLClassLoader} that can load native libraries from a
 * predefined list of directories as well as from those that are defined by
 * the java.library.path system property.
 *
 * Once a library is found an OS-handled temporary copy is created and cached
 * to maintain consistency and classloader isolation.
 *
 * This classloader handles the native library loading when the library is being loaded
 * by its logical name ({@link System#loadLibrary(String)} / {@link Runtime#loadLibrary(String)} calls).
 * For loading a native library by its absolute path, see {@link LoadNativeLibAspect}.
 */
public abstract class AbstractNativeLibHandlingClassLoader extends URLClassLoader implements OSUtil {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Directories in which to look for native libraries
     */
    protected final List<File> nativeLibDirList;
    /**
     * Used to cache (the paths of) temporary copies of loaded libraries
     */
    protected final Map<String, Path> nativeLibNameToPath = new HashMap<>();
    /**
     * Used as prefix when creating the temporary copies of libraries
     */
    private final String tmpLibFilePrefix;

    public AbstractNativeLibHandlingClassLoader(URL[] urls, List<File> initialNativeLibDirList, String tmpLibFilePrefix) {
        super(urls);

        this.nativeLibDirList = buildNativeLibDirList(initialNativeLibDirList);
        this.tmpLibFilePrefix = tmpLibFilePrefix;
    }

    public AbstractNativeLibHandlingClassLoader(URL[] urls, ClassLoader parent, List<File> initialNativeLibDirList, String tmpLibFilePrefix) {
        super(urls, parent);

        this.nativeLibDirList = buildNativeLibDirList(initialNativeLibDirList);
        this.tmpLibFilePrefix = tmpLibFilePrefix;
    }

    public static File toDir(File fileOrDir) {
        if (fileOrDir == null) {
            return null;
        } else if (fileOrDir.isFile()) {
            return fileOrDir.getParentFile();
        } else if (fileOrDir.isDirectory()) {
            return fileOrDir;
        } else {
            return null;
        }
    }

    public String findLibrary(String libname) {
        String libLocationString;

        Path libLocation = nativeLibNameToPath.compute(
                libname,
                (__, currentLocation) -> {
                    if (currentLocation != null && currentLocation.toFile().exists()) {
                        return currentLocation;
                    } else {
                        for (File nativeLibDir : nativeLibDirList) {
                            String libraryOriginalPathString = findLibrary(libname, nativeLibDir);
                            if (libraryOriginalPathString != null) {
                                return createTempCopy(libname, libraryOriginalPathString);
                            }
                        }

                        return null;
                    }
                }
        );

        if (libLocation == null) {
            libLocationString = null;
        } else {
            libLocationString = libLocation.toFile().getAbsolutePath();
        }

        return libLocationString;
    }

    protected Set<File> getUsrLibDirs() {
        Set<File> usrLibDirs = Arrays.stream(getJavaLibraryPath().split(File.pathSeparator))
                .map(String::trim)
                .filter(pathAsString -> !pathAsString.isEmpty())
                .map(File::new)
                .map(AbstractNativeLibHandlingClassLoader::toDir)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        return usrLibDirs;
    }

    protected String getJavaLibraryPath() {
        return System.getProperty("java.library.path", "");
    }

    protected Path createTempCopy(String libname, String libraryOriginalPathString) {
        Path tempFile;

        try {
            tempFile = Files.createTempFile(tmpLibFilePrefix + "_", "_" + libname);
            Files.copy(Paths.get(libraryOriginalPathString), tempFile, REPLACE_EXISTING);
        } catch (Exception e) {
            logger.error("Couldn't create temporary copy of the library '" + libname + "' found at '" + libraryOriginalPathString + "'", e);

            tempFile = null;
        }

        return tempFile;
    }

    protected String findLibrary(String libname, File nativeLibDir) {
        final File dllFile = new File(nativeLibDir, libname + ".dll");
        final File dylibFile = new File(nativeLibDir, libname + ".dylib");
        final File libdylibFile = new File(nativeLibDir, "lib" + libname + ".dylib");
        final File libsoFile = new File(nativeLibDir, "lib" + libname + ".so");
        final File soFile = new File(nativeLibDir, libname + ".so");

        if (isOsWindows() && dllFile.exists()) {
            return dllFile.getAbsolutePath();
        } else if (isOsMac()) {
            if (dylibFile.exists()) {
                return dylibFile.getAbsolutePath();
            } else if (libdylibFile.exists()) {
                return libdylibFile.getAbsolutePath();
            } else if (soFile.exists()) {
                return soFile.getAbsolutePath();
            } else if (libsoFile.exists()) {
                return libsoFile.getAbsolutePath();
            }
        } else if (isOsLinuxUnix()) {
            if (soFile.exists()) {
                return soFile.getAbsolutePath();
            } else if (libsoFile.exists()) {
                return libsoFile.getAbsolutePath();
            }
        }

        // not found in the nar. try system native dir
        return null;
    }

    private List<File> buildNativeLibDirList(List<File> initialNativeLibDirList) {
        List<File> allNativeLibDirList = new ArrayList<>(initialNativeLibDirList);

        allNativeLibDirList.addAll(getUsrLibDirs());

        return Collections.unmodifiableList(allNativeLibDirList);
    }
}
