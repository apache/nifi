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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public interface NativeLibFinder {
    Logger LOGGER = LoggerFactory.getLogger(NativeLibFinder.class);

    String OS = System.getProperty("os.name").toLowerCase();

    List<File> getNativeLibDirs();

    Map<String, Path> getNativeLibNameToPath();

    String getTmpLibFilePrefix();

    default Set<File> getUsrLibDirs() {
        Set<File> usrLibDirs = Arrays.stream(getJavaLibraryPath().split(File.pathSeparator))
                .map(String::trim)
                .filter(pathAsString -> !pathAsString.isEmpty())
                .map(File::new)
                .map(toDir())
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        return usrLibDirs;
    }

    default String getJavaLibraryPath() {
        return System.getProperty("java.library.path", "");
    }

    default Function<File, File> toDir() {
        return file -> {
            if (file.isFile()) {
                return file.getParentFile();
            } else if (file.isDirectory()) {
                return file;
            } else {
                return null;
            }
        };
    }

    default String findLibrary(String libname) {
        Path libLocation = getNativeLibNameToPath().compute(
                libname,
                (__, currentLocation) ->
                        Optional.ofNullable(currentLocation)
                                .filter(Objects::nonNull)
                                .filter(location -> location.toFile().exists())
                                .orElseGet(
                                        () -> getNativeLibDirs().stream()
                                                .map(nativeLibDir -> findLibrary(libname, nativeLibDir))
                                                .filter(Objects::nonNull)
                                                .findFirst()
                                                .map(libraryOriginalPathString -> createTempCopy(libname, libraryOriginalPathString))
                                                .orElse(null)
                                )
        );

        String libLocationString = Optional.ofNullable(libLocation)
                .map(Path::toFile)
                .map(File::getAbsolutePath)
                .orElse(null);

        return libLocationString;
    }

    default Path createTempCopy(String libname, String libraryOriginalPathString) {
        Path tempFile;

        try {
            tempFile = Files.createTempFile(getTmpLibFilePrefix() + "_", "_" + libname);
            Files.copy(Paths.get(libraryOriginalPathString), tempFile, REPLACE_EXISTING);
        } catch (Exception e) {
            LOGGER.error("Couldn't create temporary copy of the library '" + libname + "' found at '" + libraryOriginalPathString + "'", e);

            tempFile = null;
        }

        return tempFile;
    }

    default String findLibrary(String libname, File nativeLibDir) {
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


    default boolean isOsWindows() {
        return (OS.indexOf("win") >= 0);
    }

    default boolean isOsMac() {
        return (OS.indexOf("mac") >= 0);
    }

    default boolean isOsLinuxUnix() {
        return (OS.indexOf("nix") >= 0 || OS.indexOf("nux") >= 0 || OS.indexOf("aix") > 0);
    }
}
