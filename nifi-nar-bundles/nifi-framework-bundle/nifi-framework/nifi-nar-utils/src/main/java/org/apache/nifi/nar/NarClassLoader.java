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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * A <tt>ClassLoader</tt> for loading NARs (NiFi archives). NARs are designed to
 * allow isolating bundles of code (comprising one-or-more NiFi
 * <tt>FlowFileProcessor</tt>s, <tt>FlowFileComparator</tt>s and their
 * dependencies) from other such bundles; this allows for dependencies and
 * processors that require conflicting, incompatible versions of the same
 * dependency to run in a single instance of NiFi.</p>
 *
 * <p>
 * <tt>NarClassLoader</tt> follows the delegation model described in
 * {@link ClassLoader#findClass(java.lang.String) ClassLoader.findClass(...)};
 * classes are first loaded from the parent <tt>ClassLoader</tt>, and only if
 * they cannot be found there does the <tt>NarClassLoader</tt> provide a
 * definition. Specifically, this means that resources are loaded from NiFi's
 * <tt>conf</tt>
 * and <tt>lib</tt> directories first, and if they cannot be found there, are
 * loaded from the NAR.</p>
 *
 * <p>
 * The packaging of a NAR is such that it is a ZIP file with the following
 * directory structure:
 *
 * <pre>
 *   +META-INF/
 *   +-- bundled-dependencies/[native]
 *   +-- &lt;JAR files&gt;
 *   +-- MANIFEST.MF
 * </pre>
 * </p>
 *
 * The optional "native" subdirectory under "bundled-dependencies" may contain native
 * libraries. Directories defined via the java.library.path system property are also scanned.
 * After a library is found an OS-handled temporary copy is created and cached before loading
 * it to maintain consistency and classloader isolation.
 *
 * <p>
 * The MANIFEST.MF file contains the same information as a typical JAR file but
 * also includes two additional NiFi properties: {@code Nar-Id} and
 * {@code Nar-Dependency-Id}.
 * </p>
 *
 * <p>
 * The {@code Nar-Id} provides a unique identifier for this NAR.
 * </p>
 *
 * <p>
 * The {@code Nar-Dependency-Id} is optional. If provided, it indicates that
 * this NAR should inherit all of the dependencies of the NAR with the provided
 * ID. Often times, the NAR that is depended upon is referred to as the Parent.
 * This is because its ClassLoader will be the parent ClassLoader of the
 * dependent NAR.
 * </p>
 *
 * <p>
 * If a NAR is built using NiFi's Maven NAR Plugin, the {@code Nar-Id} property
 * will be set to the artifactId of the NAR. The {@code Nar-Dependency-Id} will
 * be set to the artifactId of the NAR that is depended upon. For example, if
 * NAR A is defined as such:
 *
 * <pre>
 * ...
 * &lt;artifactId&gt;nar-a&lt;/artifactId&gt;
 * &lt;packaging&gt;nar&lt;/packaging&gt;
 * ...
 * &lt;dependencies&gt;
 *   &lt;dependency&gt;
 *     &lt;groupId&gt;group&lt;/groupId&gt;
 *     &lt;artifactId&gt;nar-z&lt;/artifactId&gt;
 *     <b>&lt;type&gt;nar&lt;/type&gt;</b>
 *   &lt;/dependency&gt;
 * &lt;/dependencies&gt;
 * </pre>
 * </p>
 *
 *
 * <p>
 * Then the MANIFEST.MF file that is created for NAR A will have the following
 * properties set:
 * <ul>
 * <li>{@code Nar-Id: nar-a}</li>
 * <li>{@code Nar-Dependency-Id: nar-z}</li>
 * </ul>
 * </p>
 *
 * <p>
 * Note, above, that the {@code type} of the dependency is set to {@code nar}.
 * </p>
 *
 * <p>
 * If the NAR has more than one dependency of {@code type} {@code nar}, then the
 * Maven NAR plugin will fail to build the NAR.
 * </p>
 */
public class NarClassLoader extends AbstractNativeLibHandlingClassLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(NarClassLoader.class);

    private static final FileFilter JAR_FILTER = new FileFilter() {
        @Override
        public boolean accept(File pathname) {
            final String nameToTest = pathname.getName().toLowerCase();
            return nameToTest.endsWith(".jar") && pathname.isFile();
        }
    };

    /**
     * The NAR for which this <tt>ClassLoader</tt> is responsible.
     */
    private final File narWorkingDirectory;

    /**
     * Construct a nar class loader.
     *
     * @param narWorkingDirectory directory to explode nar contents to
     * @throws IllegalArgumentException if the NAR is missing the Java Services
     * API file for <tt>FlowFileProcessor</tt> implementations.
     * @throws ClassNotFoundException if any of the <tt>FlowFileProcessor</tt>
     * implementations defined by the Java Services API cannot be loaded.
     * @throws IOException if an error occurs while loading the NAR.
     */
    public NarClassLoader(final File narWorkingDirectory) throws ClassNotFoundException, IOException {
        super(new URL[0], initNativeLibDirList(narWorkingDirectory), narWorkingDirectory.getName());
        this.narWorkingDirectory = narWorkingDirectory;

        // process the classpath
        updateClasspath(narWorkingDirectory);
    }

    /**
     * Construct a nar class loader with the specific parent.
     *
     * @param narWorkingDirectory directory to explode nar contents to
     * @param parentClassLoader parent class loader of this nar
     * @throws IllegalArgumentException if the NAR is missing the Java Services
     * API file for <tt>FlowFileProcessor</tt> implementations.
     * @throws ClassNotFoundException if any of the <tt>FlowFileProcessor</tt>
     * implementations defined by the Java Services API cannot be loaded.
     * @throws IOException if an error occurs while loading the NAR.
     */
    public NarClassLoader(final File narWorkingDirectory, final ClassLoader parentClassLoader) throws ClassNotFoundException, IOException {
        super(new URL[0], parentClassLoader, initNativeLibDirList(narWorkingDirectory), narWorkingDirectory.getName());
        this.narWorkingDirectory = narWorkingDirectory;

        // process the classpath
        updateClasspath(narWorkingDirectory);
    }

    public File getWorkingDirectory() {
        return narWorkingDirectory;
    }

    /**
     * Adds URLs for the resources unpacked from this NAR:
     * <ul><li>the root: for classes, <tt>NAR-INF</tt>, etc.</li>
     * <li><tt>NAR-INF/bundled-dependencies</tt>: for config files, <tt>.so</tt>s,
     * etc.</li>
     * <li><tt>NAR-INF/bundled-dependencies/*.jar</tt>: for dependent
     * libraries</li></ul>
     *
     * @param root the root directory of the unpacked NAR.
     * @throws IOException if the URL list could not be updated.
     */
    private void updateClasspath(File root) throws IOException {
        addURL(root.toURI().toURL()); // for compiled classes, WEB-INF, NAR-INF/, etc.

        File dependencies = new File(root, "NAR-INF/bundled-dependencies");
        if (!dependencies.isDirectory()) {
            LOGGER.warn(narWorkingDirectory + " does not contain NAR-INF/bundled-dependencies!");
        }
        addURL(dependencies.toURI().toURL());
        if (dependencies.isDirectory()) {
            final File[] jarFiles = dependencies.listFiles(JAR_FILTER);
            if (jarFiles != null) {
                Arrays.sort(jarFiles, Comparator.comparing(File::getName));
                for (File libJar : jarFiles) {
                    addURL(libJar.toURI().toURL());
                }
            }
        }
    }

    public File getNARNativeLibDir() {
        return getNARNativeLibDir(narWorkingDirectory);
    }

    private static List<File> initNativeLibDirList(File narWorkingDirectory) {
        ArrayList<File> nativeLibDirList = new ArrayList<>();

        nativeLibDirList.add(getNARNativeLibDir(narWorkingDirectory));

        return nativeLibDirList;
    }

    private static File getNARNativeLibDir(File narWorkingDirectory) {
        File dependencies = new File(narWorkingDirectory, "NAR-INF/bundled-dependencies");
        if (!dependencies.isDirectory()) {
            LOGGER.warn(narWorkingDirectory + " does not contain NAR-INF/bundled-dependencies!");
        }

        return new File(dependencies, "native");
    }

    @Override
    public String toString() {
        return NarClassLoader.class.getName() + "[" + narWorkingDirectory.getPath() + "]";
    }
}
