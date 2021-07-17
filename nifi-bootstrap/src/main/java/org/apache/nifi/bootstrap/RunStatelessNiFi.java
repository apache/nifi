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
package org.apache.nifi.bootstrap;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RunStatelessNiFi {

    public static void main(final String[] args) throws Exception {

        String nifi_home = System.getenv("NIFI_HOME");
        if (nifi_home == null || nifi_home.equals("")) {
            nifi_home = ".";
        }

        final List<URL> cpURLs = new ArrayList<>();
        final File libDir = new File(nifi_home + "/lib");
        if (libDir.exists()) {
            for (final File file : Objects.requireNonNull(libDir.listFiles((dir, filename) -> filename.toLowerCase().endsWith(".jar")))) {
                cpURLs.add(file.toURI().toURL());
            }
        }

        if (cpURLs.isEmpty()) {
            throw new RuntimeException("Could not find lib directory at " + libDir.getAbsolutePath());
        }

        String runtimeJavaVersion = System.getProperty("java.version");
        if (Integer.parseInt(runtimeJavaVersion.substring(0, runtimeJavaVersion.indexOf('.'))) >= 11) {
            /* If running on Java 11 or greater, add the JAXB/activation/annotation libs to the classpath.
             *
             * TODO: Once the minimum Java version requirement of NiFi is 11, this processing should be removed.
             * JAXB/activation/annotation will be added as an actual dependency via pom.xml.
             */
            final File libJava11Dir = new File(nifi_home + "/lib/java11");
            if (libJava11Dir.exists()) {
                for (final File file : Objects.requireNonNull(libJava11Dir.listFiles((dir, filename) -> filename.toLowerCase().endsWith(".jar")))) {
                    cpURLs.add(file.toURI().toURL());
                }
            }
        }

        final URLClassLoader rootClassLoader = new URLClassLoader(cpURLs.toArray(new URL[0]));
        Thread.currentThread().setContextClassLoader(rootClassLoader);

        final Class<?> programClass = Class.forName("org.apache.nifi.StatelessNiFi", true, rootClassLoader);
        final Method launchMethod = programClass.getMethod("main", String[].class);
        launchMethod.setAccessible(true);
        launchMethod.invoke(null, (Object) args);
    }
}
