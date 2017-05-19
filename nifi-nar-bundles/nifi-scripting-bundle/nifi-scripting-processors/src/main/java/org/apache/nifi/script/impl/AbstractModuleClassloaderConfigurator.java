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
package org.apache.nifi.script.impl;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.script.ScriptEngineConfigurator;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

/**
 * This base class provides a common implementation for the getModuleURLsForClasspath method of the
 * ScriptEngineConfigurator interface
 */
public abstract class AbstractModuleClassloaderConfigurator implements ScriptEngineConfigurator {

    /**
     * Scans the given module paths for JARs. The path itself (whether a directory or file) will be added to the list
     * of returned module URLs, and if a directory is specified, it is scanned for JAR files (files ending with .jar).
     * Any JAR files found are added to the list of module URLs. This is a convenience method for adding directories
     * full of JAR files to an ExecuteScript or InvokeScriptedProcessor instance, rather than having to enumerate each
     * JAR's URL.
     * @param modulePaths An array of module paths to scan/add
     * @param log A logger for the calling component, to provide feedback for missing files, e.g.
     * @return An array of URLs corresponding to all modules determined from the input set of module paths.
     */
    @Override
    public URL[] getModuleURLsForClasspath(String[] modulePaths, ComponentLog log) {
        List<URL> additionalClasspath = new LinkedList<>();
        if (modulePaths != null) {
            for (String modulePathString : modulePaths) {
                File modulePath = new File(modulePathString);

                if (modulePath.exists()) {
                    // Add the URL of this path
                    try {
                        additionalClasspath.add(modulePath.toURI().toURL());
                    } catch (MalformedURLException mue) {
                        log.warn("{} is not a valid file/folder, ignoring", new Object[]{modulePath.getAbsolutePath()}, mue);
                    }

                    // If the path is a directory, we need to scan for JARs and add them to the classpath
                    if (modulePath.isDirectory()) {
                        File[] jarFiles = modulePath.listFiles(new FilenameFilter() {
                            @Override
                            public boolean accept(File dir, String name) {
                                return (name != null && name.endsWith(".jar"));
                            }
                        });

                        if (jarFiles != null) {
                            // Add each to the classpath
                            for (File jarFile : jarFiles) {
                                try {
                                    additionalClasspath.add(jarFile.toURI().toURL());

                                } catch (MalformedURLException mue) {
                                    log.warn("{} is not a valid file/folder, ignoring", new Object[]{modulePath.getAbsolutePath()}, mue);
                                }
                            }
                        }
                    }
                } else {
                    log.warn("{} does not exist, ignoring", new Object[]{modulePath.getAbsolutePath()});
                }
            }
        }
        return additionalClasspath.toArray(new URL[additionalClasspath.size()]);
    }
}
