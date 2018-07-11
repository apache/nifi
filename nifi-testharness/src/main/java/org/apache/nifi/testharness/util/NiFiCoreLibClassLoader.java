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
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public final class NiFiCoreLibClassLoader extends URLClassLoader {


    public NiFiCoreLibClassLoader(File nifiHomeDir, ClassLoader parent) {
        super(getURls(nifiHomeDir), parent);
    }

    private static URL[] getURls(File nifiHomeDir) {

        try {
            File libDir = new File(nifiHomeDir, "lib");
            File bootstrapLibDir = new File(libDir, "bootstrap");


            List<URL> libs = Files.list(libDir.toPath())
                    .filter(NiFiCoreLibClassLoader::isJarOrNarFile)
                    .map(NiFiCoreLibClassLoader::toURL)
                    .collect(Collectors.toList());
            List<URL> bootstrapLibs = Files.list(bootstrapLibDir.toPath())
                    .filter(NiFiCoreLibClassLoader::isJarOrNarFile)
                    .map(NiFiCoreLibClassLoader::toURL)
                    .collect(Collectors.toList());

            LinkedList<URL> urls = new LinkedList<>();
            urls.addAll(libs);
            urls.addAll(bootstrapLibs);

            return urls.toArray(new URL[urls.size()]);
        } catch (IOException ioEx) {
            throw new RuntimeException(ioEx);
        }


    }

    private static URL toURL(Path path) {
        try {
            return path.toUri().toURL();
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }

    }

    private static boolean isJarOrNarFile(Path path) {
        String fullPathString = path.getFileName().toString();

        return path.toFile().isFile() && fullPathString.endsWith(".jar");
    }


}
