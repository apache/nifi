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

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class ClassLoaderUtils {

    public static ClassLoader getCustomClassLoader(String modulePath, ClassLoader parentClassLoader, FilenameFilter filenameFilter) throws MalformedURLException {
        // Split and trim the module path(s)
        List<String> modules = (modulePath == null)
                ? null
                : Arrays.stream(modulePath.split(",")).filter(StringUtils::isNotBlank).map(String::trim).collect(Collectors.toList());

        URL[] classpaths = getURLsForClasspath(modules, filenameFilter);
        return createModuleClassLoader(classpaths, parentClassLoader);
    }

    protected static URL[] getURLsForClasspath(List<String> modulePaths, FilenameFilter filenameFilter) throws MalformedURLException {
        List<URL> additionalClasspath = new LinkedList<>();
        if (modulePaths != null) {
            for (String modulePathString : modulePaths) {
                // If the path is already a URL, just add it (but don't check if it exists, too expensive and subject to network availability)
                boolean isUrl = true;
                try {
                    additionalClasspath.add(new URL(modulePathString));
                } catch (MalformedURLException mue) {
                    isUrl = false;
                }
                if (!isUrl) {
                    File modulePath = new File(modulePathString);

                    if (modulePath.exists()) {

                        additionalClasspath.add(modulePath.toURI().toURL());

                        if (modulePath.isDirectory()) {
                            File[] files = modulePath.listFiles(filenameFilter);

                            if (files != null) {
                                for (File jarFile : files) {
                                    additionalClasspath.add(jarFile.toURI().toURL());
                                }
                            }
                        }
                    } else {
                        throw new MalformedURLException("Path specified does not exist");
                    }
                }
            }
        }
        return additionalClasspath.toArray(new URL[additionalClasspath.size()]);
    }

    protected static ClassLoader createModuleClassLoader(URL[] modules, ClassLoader parentClassLoader) {
        return new URLClassLoader(modules, parentClassLoader);
    }

}
