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



package org.apache.nifi.testharness.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;

public final class FileUtils {


    private static final String MAC_DS_STORE_NAME = ".DS_Store";

    private FileUtils() {
        // no instances
    }

    public static void deleteDirectoryRecursive(Path directory) throws IOException {
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public static void deleteDirectoryRecursive(File dir) {
        try {
            deleteDirectoryRecursive(dir.toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void createLink(Path newLink, Path existingFile)  {
        try {
            Files.createSymbolicLink(newLink, existingFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void createSymlinks(File newLinkDir, File existingDir) {
        Arrays.stream(existingDir.list())
                .filter(fileName -> !MAC_DS_STORE_NAME.equals(fileName))
                .forEach(fileName -> {
                    Path newLink = Paths.get(newLinkDir.getAbsolutePath(), fileName);
                    Path existingFile = Paths.get(existingDir.getAbsolutePath(), fileName);

                    File symlinkFile = newLink.toFile();
                    if (symlinkFile.exists()) {
                        symlinkFile.delete();
                    }

                    createLink(newLink, existingFile);
                });
    }
}
