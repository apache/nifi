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
package org.apache.nifi.dbcp.utils;

import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.components.resource.ResourceReferences;
import org.apache.nifi.components.resource.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Driver;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;

public class DriverUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DriverUtils.class);
    private static final String DRIVER_CLASS_PATTERN = "(?i).*(driver|jdbc).*";
    private static final Pattern DRIVER_PATTERN = Pattern.compile(DRIVER_CLASS_PATTERN);

    /**
     * Discovers JDBC driver classes using static JAR scanning only (no class
     * loading) This is used during validation when resources aren't loaded into
     * classpath yet
     */
    public static List<String> findDriverClassNames(final ResourceReferences driverResources) {
        final Set<String> driverClasses = new TreeSet<>();

        if (driverResources == null || driverResources.getCount() == 0) {
            LOGGER.debug("No driver resources provided for static discovery");
            return new ArrayList<>();
        }
        LOGGER.debug("Starting static driver discovery for {} resources", driverResources.getCount());

        for (final ResourceReference resource : driverResources.flattenRecursively().asList()) {
            try {
                LOGGER.debug("Processing resource: {} (type: {})", resource.getLocation(), resource.getResourceType());
                final List<File> jarFiles = getJarFilesFromResource(resource);
                LOGGER.debug("Found {} JAR files in resource {}", jarFiles.size(), resource.getLocation());
                for (final File jarFile : jarFiles) {
                    LOGGER.debug("Scanning JAR file: {}", jarFile.getAbsolutePath());
                    final Set<String> foundDrivers = scanJarForDriverClassesUsingMetadata(jarFile);
                    LOGGER.debug("Found {} potential driver classes in {}", foundDrivers.size(), jarFile.getName());
                    driverClasses.addAll(foundDrivers);
                }
            } catch (final Exception e) {
                LOGGER.warn("Error processing resource {} for static driver discovery", resource.getLocation(), e);
            }
        }

        LOGGER.debug("Static driver discovery completed. Found {} potential driver classes", driverClasses.size());
        return new ArrayList<>(driverClasses);
    }

    /**
     * Scans a JAR file for potential JDBC driver class names using static analysis
     * only This uses improved heuristics including checking META-INF/services and
     * common patterns
     */
    private static Set<String> scanJarForDriverClassesUsingMetadata(final File jarFile) {
        final Set<String> driverClasses = new TreeSet<>();
        try (final JarFile jar = new JarFile(jarFile)) {
            // Check META-INF/services/java.sql.Driver for registered drivers
            // This is the most reliable method
            final JarEntry servicesEntry = jar.getJarEntry("META-INF/services/java.sql.Driver");
            if (servicesEntry != null) {
                try (Scanner scanner = new Scanner(jar.getInputStream(servicesEntry))) {
                    while (scanner.hasNextLine()) {
                        final String line = scanner.nextLine().trim();
                        if (!line.isEmpty() && !line.startsWith("#")) {
                            driverClasses.add(line);
                            LOGGER.debug("Found driver in META-INF/services: {}", line);
                        }
                    }
                }
            }
            return driverClasses;
        } catch (final Exception e) {
            LOGGER.warn("Error scanning JAR file {} for driver classes", jarFile.getAbsolutePath(), e);
        }
        return driverClasses;
    }

    /**
     * Gets JAR files from a ResourceReference for scanning. It is expected that the
     * submitted resource is coming from the listing of
     * driverResources.flattenRecursively().asList() so that this method does not
     * need to be recursive
     */
    private static List<File> getJarFilesFromResource(final ResourceReference resource) {
        final List<File> jarFiles = new ArrayList<>();
        if (resource.getResourceType() == ResourceType.FILE) {
            final File file = new File(resource.getLocation());
            if (file.exists() && file.canRead() && file.getName().toLowerCase().endsWith(".jar")) {
                jarFiles.add(file);
            }
        }
        // we don't need to scan for directory since we already did a recursive flatten
        // and we don't want to deal with URLs in this case
        return jarFiles;
    }

    /**
     * Discovers all potential JDBC driver classes in the provided resources Since
     * dynamicallyModifiesClasspath=true, we scan the resources directly rather than
     * trying to load classes, as NiFi handles classpath modification
     */
    public static List<String> discoverDriverClasses(final ResourceReferences driverResources) {
        final Set<String> driverClasses = new TreeSet<>(); // Use TreeSet for sorted results

        if (driverResources == null || driverResources.getCount() == 0) {
            return new ArrayList<>();
        }

        for (final ResourceReference resource : driverResources.flattenRecursively().asList()) {
            try {
                final List<File> jarFiles = DriverUtils.getJarFilesFromResource(resource);
                for (final File jarFile : jarFiles) {
                    driverClasses.addAll(scanJarForDriverClasses(jarFile));
                }
            } catch (final Exception e) {
                LOGGER.warn("Error processing resource {} for driver discovery", resource.getLocation(), e);
            }
        }

        return new ArrayList<>(driverClasses);
    }

    /**
     * Scans a JAR file for potential JDBC driver class names (without loading them)
     */
    private static Set<String> scanJarForDriverClasses(final File jarFile) {
        final Set<String> actualDriverClasses = new TreeSet<>();

        try (JarFile jar = new JarFile(jarFile)) {
            final Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                final JarEntry entry = entries.nextElement();
                final String entryName = entry.getName();

                // Look for .class files (exclude inner classes)
                if (entryName.endsWith(".class") && !entryName.contains("$")) {
                    String className = entryName.substring(0, entryName.length() - 6).replace('/', '.');

                    // First filter with heuristics to avoid loading every class
                    if (isPotentialDriverClass(className)) {
                        // Now actually verify it's a JDBC driver
                        if (isActualDriverClass(className)) {
                            actualDriverClasses.add(className);
                        }
                    }
                }
            }
        } catch (final Exception e) {
            LOGGER.warn("Error scanning JAR file {} for driver classes", jarFile.getAbsolutePath(), e);
        }

        return actualDriverClasses;
    }

    /**
     * Uses heuristics to identify potential JDBC driver classes without loading
     * them This is a first-pass filter to avoid loading every single class
     */
    private static boolean isPotentialDriverClass(final String className) {
        return DRIVER_PATTERN.matcher(className).matches();
    }

    /**
     * Actually loads and checks if a class implements java.sql.Driver Since
     * dynamicallyModifiesClasspath=true, the class should be available
     */
    private static boolean isActualDriverClass(final String className) {
        try {
            final Class<?> clazz = Class.forName(className);

            // Check if it implements Driver interface and is not abstract/interface
            return Driver.class.isAssignableFrom(clazz)
                    && !clazz.isInterface()
                    && !java.lang.reflect.Modifier.isAbstract(clazz.getModifiers());
        } catch (final Throwable e) {
            // Class couldn't be loaded or has issues - not a valid driver
            return false;
        }
    }

}
