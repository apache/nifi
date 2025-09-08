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

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.util.FileUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

public final class NarUnpacker {
    public static final String BUNDLED_DEPENDENCIES_DIRECTORY = "NAR-INF/bundled-dependencies";

    private static final String BUNDLED_DEPENDENCIES_PREFIX = "META-INF/bundled-dependencies";

    private static final Logger logger = LoggerFactory.getLogger(NarUnpacker.class);
    private static final String HASH_FILENAME = "nar-digest";
    private static final FileFilter NAR_FILTER = pathname -> {
        final String nameToTest = pathname.getName().toLowerCase();
        return nameToTest.endsWith(".nar") && pathname.isFile();
    };

    public static ExtensionMapping unpackNars(final NiFiProperties props, final Bundle systemBundle, final NarUnpackMode unpackMode) {
        // Default to NiFi's framework NAR ID if not given
        return unpackNars(props, NarClassLoaders.FRAMEWORK_NAR_ID, systemBundle, unpackMode);
    }

    public static ExtensionMapping unpackNars(final NiFiProperties props, final String frameworkNarId, final Bundle systemBundle, final NarUnpackMode unpackMode) {
        final List<Path> narLibraryDirs = props.getNarLibraryDirectories();
        final File frameworkWorkingDir = props.getFrameworkWorkingDirectory();
        final File extensionsWorkingDir = props.getExtensionsWorkingDirectory();

        return unpackNars(systemBundle, frameworkWorkingDir, frameworkNarId, extensionsWorkingDir, narLibraryDirs, unpackMode);
    }

    public static ExtensionMapping unpackNars(final Bundle systemBundle, final File frameworkWorkingDir, final String frameworkNarId,
                                              final File extensionsWorkingDir, final List<Path> narLibraryDirs, final NarUnpackMode unpackMode) {
        return unpackNars(systemBundle, frameworkWorkingDir, extensionsWorkingDir, narLibraryDirs, true, frameworkNarId, true, true, unpackMode, (coordinate) -> true);
    }

    public static ExtensionMapping unpackNars(final Bundle systemBundle, final File frameworkWorkingDir, final File extensionsWorkingDir, final List<Path> narLibraryDirs,
                                              final boolean requireFrameworkNar, final String frameworkNarId,
                                              final boolean requireJettyNar, final boolean verifyHash, final NarUnpackMode unpackMode,
                                              final Predicate<BundleCoordinate> narFilter) {
        try {
            File unpackedJetty = null;
            File unpackedFramework = null;
            final Set<File> unpackedExtensions = new HashSet<>();
            final List<File> narFiles = new ArrayList<>();

            // make sure the nar directories are there and accessible
            if (requireFrameworkNar) {
                FileUtils.ensureDirectoryExistAndCanReadAndWrite(frameworkWorkingDir);
            }

            FileUtils.ensureDirectoryExistAndCanReadAndWrite(extensionsWorkingDir);

            for (Path narLibraryDir : narLibraryDirs) {

                File narDir = narLibraryDir.toFile();

                // Test if the source NARs can be read
                FileUtils.ensureDirectoryExistAndCanRead(narDir);

                File[] dirFiles = narDir.listFiles(NAR_FILTER);
                if (dirFiles != null) {
                    List<File> fileList = Arrays.asList(dirFiles);
                    narFiles.addAll(fileList);
                }
            }

            if (!narFiles.isEmpty()) {
                final long startTime = System.nanoTime();
                logger.info("Expanding {} NAR files started", narFiles.size());
                for (File narFile : narFiles) {
                    if (!narFile.canRead()) {
                        throw new IllegalStateException("Unable to read NAR file: " + narFile.getAbsolutePath());
                    }

                    logger.debug("Expanding NAR file: {}", narFile.getAbsolutePath());

                    // get the manifest for this nar
                    try (final JarFile nar = new JarFile(narFile)) {
                        BundleCoordinate bundleCoordinate = createBundleCoordinate(nar.getManifest());

                        if (!narFilter.test(bundleCoordinate)) {
                            logger.debug("Will not expand NAR {} because it does not match the provided filter", bundleCoordinate);
                            continue;
                        }

                        // determine if this is the framework
                        if (frameworkNarId != null && frameworkNarId.equals(bundleCoordinate.getId())) {
                            if (unpackedFramework != null) {
                                throw new IllegalStateException("Multiple framework NARs discovered. Only one framework is permitted.");
                            }

                            // unpack the framework nar
                            unpackedFramework = unpackNar(narFile, frameworkWorkingDir, verifyHash, unpackMode);
                        } else if (NarClassLoaders.JETTY_NAR_ID.equals(bundleCoordinate.getId())) {
                            if (unpackedJetty != null) {
                                throw new IllegalStateException("Multiple Jetty NARs discovered. Only one Jetty NAR is permitted.");
                            }

                            // unpack and record the Jetty nar
                            unpackedJetty = unpackNar(narFile, extensionsWorkingDir, verifyHash, unpackMode);
                            unpackedExtensions.add(unpackedJetty);
                        } else {
                            // unpack and record the extension nar
                            final File unpackedExtension = unpackNar(narFile, extensionsWorkingDir, verifyHash, unpackMode);
                            unpackedExtensions.add(unpackedExtension);
                        }
                    }
                }

                if (requireFrameworkNar) {
                    // ensure we've found the framework nar
                    if (unpackedFramework == null) {
                        throw new IllegalStateException("No framework NAR found.");
                    } else if (!unpackedFramework.canRead()) {
                        throw new IllegalStateException("Framework NAR cannot be read.");
                    }
                }

                if (requireJettyNar) {
                    // ensure we've found the jetty nar
                    if (unpackedJetty == null) {
                        throw new IllegalStateException("No Jetty NAR found.");
                    } else if (!unpackedJetty.canRead()) {
                        throw new IllegalStateException("Jetty NAR cannot be read.");
                    }
                }

                // Determine if any nars no longer exist and delete their working directories. This happens
                // if a new version of a nar is dropped into the lib dir. ensure no old framework are present
                if (unpackedFramework != null && frameworkWorkingDir != null) {
                    final File[] frameworkWorkingDirContents = frameworkWorkingDir.listFiles();
                    if (frameworkWorkingDirContents != null) {
                        for (final File unpackedNar : frameworkWorkingDirContents) {
                            if (!unpackedFramework.equals(unpackedNar)) {
                                FileUtils.deleteFile(unpackedNar, true);
                            }
                        }
                    }
                }

                // ensure no old extensions are present
                final File[] extensionsWorkingDirContents = extensionsWorkingDir.listFiles();
                if (extensionsWorkingDirContents != null) {
                    for (final File unpackedNar : extensionsWorkingDirContents) {
                        if (!unpackedExtensions.contains(unpackedNar)) {
                            FileUtils.deleteFile(unpackedNar, true);
                        }
                    }
                }

                final long duration = System.nanoTime() - startTime;
                final double durationSeconds = TimeUnit.NANOSECONDS.toMillis(duration) / 1000.0;
                logger.info("Expanded {} NAR files in {} seconds ({} ns)", narFiles.size(), durationSeconds, duration);
            }

            final Map<File, BundleCoordinate> unpackedNars = new HashMap<>(createUnpackedNarBundleCoordinateMap(extensionsWorkingDir));

            final ExtensionMapping extensionMapping = new ExtensionMapping();
            mapExtensions(unpackedNars, extensionMapping);
            populateExtensionMapping(extensionMapping, systemBundle.getBundleDetails().getCoordinate(), systemBundle.getBundleDetails().getWorkingDirectory());

            return extensionMapping;
        } catch (IOException e) {
            logger.warn("Unable to load NAR bundles. Proceeding without loading any further NAR bundles", e);
        }

        return null;
    }

    /**
     * Creates a map containing the nar directory mapped to it's bundle-coordinate.
     * @param extensionsWorkingDir where to find extensions
     * @return map of coordinates for bundles
     */
    private static Map<File, BundleCoordinate> createUnpackedNarBundleCoordinateMap(File extensionsWorkingDir) {
        File[] unpackedDirs = extensionsWorkingDir.listFiles(file -> file.isDirectory() && file.getName().endsWith("nar-unpacked"));
        if (unpackedDirs == null) {
            return Collections.emptyMap();
        }

        final Map<File, BundleCoordinate> result = new HashMap<>();
        for (File unpackedDir : unpackedDirs) {
            Path mf = Paths.get(unpackedDir.getAbsolutePath(), "META-INF", "MANIFEST.MF");
            try (InputStream is = Files.newInputStream(mf)) {
                Manifest manifest = new Manifest(is);
                BundleCoordinate bundleCoordinate = createBundleCoordinate(manifest);
                result.put(unpackedDir, bundleCoordinate);
            } catch (IOException e) {
                logger.error("Unable to parse NAR information from unpacked directory [{}]", unpackedDir.getAbsoluteFile(), e);
            }
        }
        return result;
    }

    private static BundleCoordinate createBundleCoordinate(final Manifest manifest) {
        Attributes mainAttributes = manifest.getMainAttributes();
        String groupId = mainAttributes.getValue(NarManifestEntry.NAR_GROUP.getEntryName());
        String narId = mainAttributes.getValue(NarManifestEntry.NAR_ID.getEntryName());
        String version = mainAttributes.getValue(NarManifestEntry.NAR_VERSION.getEntryName());
        return new BundleCoordinate(groupId, narId, version);
    }

    private static void mapExtensions(final Map<File, BundleCoordinate> unpackedNars, final ExtensionMapping mapping) throws IOException {
        for (final Map.Entry<File, BundleCoordinate> entry : unpackedNars.entrySet()) {
            final File unpackedNar = entry.getKey();
            final BundleCoordinate bundleCoordinate = entry.getValue();
            mapExtension(unpackedNar, bundleCoordinate, mapping);
        }
    }

    public static void mapExtension(final File unpackedNar, final BundleCoordinate bundleCoordinate, final ExtensionMapping mapping) throws IOException {
        final File bundledDependencies = new File(unpackedNar, BUNDLED_DEPENDENCIES_DIRECTORY);
        populateExtensionMapping(mapping, bundleCoordinate, bundledDependencies);
    }

    private static void populateExtensionMapping(final ExtensionMapping mapping, final BundleCoordinate bundleCoordinate, final File bundledDirectory) throws IOException {
        final File[] directoryContents = bundledDirectory.listFiles();
        if (directoryContents != null) {
            for (final File file : directoryContents) {
                if (file.getName().toLowerCase().endsWith(".jar")) {
                    determineNiFiComponents(bundleCoordinate, file, mapping);
                }
            }
        }
    }

    /**
     * Unpacks the specified nar into the specified base working directory.
     *
     * @param nar the nar to unpack
     * @param baseWorkingDirectory the directory to unpack to
     * @param verifyHash if the NAR has already been unpacked, indicates whether or not the hash should be verified. If this value is true,
     * and the NAR's hash does not match the hash written to the unpacked directory, the working directory will be deleted and the NAR will be
     * unpacked again. If false, the NAR will not be unpacked again and its hash will not be checked.
     * @param unpackMode specifies how the contents of the NAR should be unpacked
     * @return the directory to the unpacked NAR
     * @throws IOException if unable to explode nar
     */
    public static File unpackNar(final File nar, final File baseWorkingDirectory, final boolean verifyHash, final NarUnpackMode unpackMode) throws IOException {
        final File narWorkingDirectory = new File(baseWorkingDirectory, nar.getName() + "-unpacked");

        // if the working directory doesn't exist, unpack the nar
        if (!narWorkingDirectory.exists()) {
            unpackIndividualJars(nar, narWorkingDirectory, FileDigestUtils.getDigest(nar), unpackMode);
        } else if (verifyHash) {
            // the working directory does exist. Run digest against the nar
            // file and check if the nar has changed since it was deployed.
            final byte[] narDigest = FileDigestUtils.getDigest(nar);
            final File workingHashFile = new File(narWorkingDirectory, HASH_FILENAME);
            if (!workingHashFile.exists()) {
                FileUtils.deleteFile(narWorkingDirectory, true);
                unpackIndividualJars(nar, narWorkingDirectory, narDigest, unpackMode);
            } else {
                final byte[] hashFileContents = Files.readAllBytes(workingHashFile.toPath());
                if (!Arrays.equals(hashFileContents, narDigest)) {
                    logger.info("Reloading changed NAR [{}]", nar.getAbsolutePath());
                    FileUtils.deleteFile(narWorkingDirectory, true);
                    unpackIndividualJars(nar, narWorkingDirectory, narDigest, unpackMode);
                }
            }
        } else {
            logger.debug("Directory {} already exists. Will not verify hash. Assuming nothing has changed.", narWorkingDirectory);
        }

        return narWorkingDirectory;
    }

    private static void unpackIndividualJars(final File nar, final File workingDirectory, final byte[] hash, final NarUnpackMode unpackMode) throws IOException {
        switch (unpackMode) {
            case UNPACK_INDIVIDUAL_JARS:
                unpackIndividualJars(nar, workingDirectory, hash);
                return;
            case UNPACK_TO_UBER_JAR:
                unpackToUberJar(nar, workingDirectory, hash);
        }
    }

    /**
     * Unpacks the NAR to the specified directory. Creates a checksum file that can be
     * used to determine if future expansion is necessary.
     *
     * @param workingDirectory the root directory to which the NAR should be unpacked.
     * @throws IOException if the NAR could not be unpacked.
     */
    private static void unpackIndividualJars(final File nar, final File workingDirectory, final byte[] hash) throws IOException {
        try (final JarFile jarFile = new JarFile(nar)) {
            final Enumeration<JarEntry> jarEntries = jarFile.entries();
            while (jarEntries.hasMoreElements()) {
                final JarEntry jarEntry = jarEntries.nextElement();
                final File jarEntryFile = getMappedJarEntryFile(workingDirectory, jarEntry);
                if (jarEntry.isDirectory()) {
                    FileUtils.ensureDirectoryExistAndCanReadAndWrite(jarEntryFile);
                } else {
                    makeFile(jarFile.getInputStream(jarEntry), jarEntryFile);
                }
            }
        }

        final File hashFile = new File(workingDirectory, HASH_FILENAME);
        try (final FileOutputStream fos = new FileOutputStream(hashFile)) {
            fos.write(hash);
        }
    }

    /**
     * Unpacks the NAR to a single JAR file in the specified directory. Creates a checksum file that can be
     * used to determine if future expansion is necessary.
     *
     * @param workingDirectory the root directory to which the NAR should be unpacked.
     * @throws IOException if the NAR could not be unpacked.
     */
    private static void unpackToUberJar(final File nar, final File workingDirectory, final byte[] hash) throws IOException {
        logger.debug("====================================");
        logger.debug("Unpacking NAR {}", nar.getAbsolutePath());

        final File unpackedUberJarFile = new File(workingDirectory, "NAR-INF/bundled-dependencies/" + nar.getName() + ".unpacked.uber.jar");
        Files.createDirectories(workingDirectory.toPath());
        Files.createDirectories(unpackedUberJarFile.getParentFile().toPath());

        final Set<String> entriesCreated = new HashSet<>();

        try (final JarFile jarFile = new JarFile(nar);
             final OutputStream out = new FileOutputStream(unpackedUberJarFile);
             final OutputStream bufferedOut = new BufferedOutputStream(out);
             final JarOutputStream uberJarOut = new JarOutputStream(bufferedOut)) {

            final Enumeration<JarEntry> jarEntries = jarFile.entries();
            while (jarEntries.hasMoreElements()) {
                final JarEntry jarEntry = jarEntries.nextElement();
                final File jarEntryFile = getMappedJarEntryFile(workingDirectory, jarEntry);
                logger.debug("Unpacking NAR entry {}", jarEntryFile);

                // If we've not yet created this entry, create it now. If we've already created the entry, ignore it.
                if (!entriesCreated.add(jarEntry.getName())) {
                    continue;
                }

                // Explode anything from META-INF and any WAR files into the nar's output directory instead of copying it to the uber jar.
                // The WAR files are important so that NiFi can load its UI. The META-INF/ directory is important in order to ensure that our
                // NarClassLoader has all of the information that it needs.
                final String jarEntryFilePath = jarEntryFile.getAbsolutePath();
                if (jarEntryFilePath.contains("META-INF") || (jarEntryFilePath.contains("NAR-INF") && jarEntryFilePath.endsWith(".war"))) {
                    if (jarEntry.isDirectory()) {
                        continue;
                    }

                    Files.createDirectories(jarEntryFile.getParentFile().toPath());

                    try (final InputStream entryIn = jarFile.getInputStream(jarEntry);
                         final OutputStream manifestOut = new FileOutputStream(jarEntryFile)) {
                        copy(entryIn, manifestOut);
                    }

                    continue;
                }

                if (jarEntry.isDirectory()) {
                    uberJarOut.putNextEntry(new JarEntry(jarEntry.getName()));
                } else if (jarEntryFilePath.endsWith(".jar")) {
                    // Unpack each .jar file into the uber jar, taking care to deal with META-INF/ files, etc. carefully.
                    logger.debug("Unpacking JAR {}", jarEntryFile);

                    try (final InputStream entryIn = jarFile.getInputStream(jarEntry);
                         final InputStream in = new BufferedInputStream(entryIn)) {
                        copyJarContents(in, uberJarOut, entriesCreated, workingDirectory);
                    }
                } else {
                    // Copy the entry directly from NAR to the uber jar
                    final JarEntry fileEntry = new JarEntry(jarEntry.getName());
                    uberJarOut.putNextEntry(fileEntry);

                    try (final InputStream entryIn = jarFile.getInputStream(jarEntry);
                         final InputStream in = new BufferedInputStream(entryIn)) {
                        copy(in, uberJarOut);
                    }

                    uberJarOut.closeEntry();
                }
            }
        }

        final File hashFile = new File(workingDirectory, HASH_FILENAME);
        try (final FileOutputStream fos = new FileOutputStream(hashFile)) {
            fos.write(hash);
        }
    }

    /**
     * Copies the contents of the Jar File whose input stream is provided to the JarOutputStream provided. Any META-INF files will be expanded into the
     * appropriate location of the Working Directory. Other entries will be copied to the Jar Output Stream.
     *
     * @param in the InputStream from a jar file
     * @param out the OutputStream to write the contents to
     * @param entriesCreated the Set of all entries that have been created for the output Jar File. Any newly added entries will be added to this Set, so it must be mutable.
     * @param workingDirectory the working directory for the nar
     * @throws IOException if unable to copy the jar's entries
     */
    private static void copyJarContents(final InputStream in, final JarOutputStream out, final Set<String> entriesCreated, final File workingDirectory) throws IOException {
        try (final JarInputStream jarInputStream = new JarInputStream(in)) {
            JarEntry jarEntry;
            while ((jarEntry = jarInputStream.getNextJarEntry()) != null) {
                final String entryName = jarEntry.getName();
                final File outFile = getJarEntryFile(workingDirectory, entryName);

                // The META-INF/ directory can contain several different types of files. For example, it contains:
                // MANIFEST.MF
                // LICENSE
                // NOTICE
                // Service Loader configuration
                // Spring Handler configuration
                //
                // Of these, the License/Notice isn't particularly critical because this is a temporary file that's being created and loaded, not a file that is
                // distributed. The Service Loader configurtion, Spring Handler, etc. can be dealt with by simply concatenating the contents together.
                // But the MANIFEST.MF file is special. If it's not properly formed, it will prefer the ClassLoader from loading the JAR file, and we can't simply
                // concatenate the files together. However, it's not required and generally contains information that we don't care about in this context. So we can
                // simply ignore it.
                if ((entryName.contains("META-INF/") && !entryName.contains("META-INF/MANIFEST.MF") ) && !jarEntry.isDirectory()) {
                    logger.debug("Found META-INF/services file {}", entryName);

                    // Because we're combining multiple jar files into one, we can run into situations where there may be conflicting filenames
                    // such as 1 jar has a file named META-INF/license and another jar file has a META-INF/license/my-license.txt. We can generally
                    // just ignore these, though, as they are not necessary in this temporarily created jar file. So we log it at a debug level and
                    // move on.
                    final File outDir = outFile.getParentFile();
                    if (!outDir.exists() && !outDir.mkdirs()) {
                        logger.debug("Skipping unpacking {} because parent file does not exist and could not be created", outFile);
                        continue;
                    }
                    if (!outDir.isDirectory()) {
                        logger.debug("Skipping unpacking {} because parent file is not a directory", outFile);
                        continue;
                    }

                    // Write to file, appending to the existing file if it already exists.
                    try (final OutputStream metaInfFileOut = new FileOutputStream(outFile, true);
                         final OutputStream bufferedOut = new BufferedOutputStream(metaInfFileOut)) {
                        copy(jarInputStream, bufferedOut);
                        bufferedOut.write("\n".getBytes(StandardCharsets.UTF_8));
                    }

                    // Move to the next entry.
                    continue;
                }

                // If the entry already exists, do not try to create another entry with the same name. Just skip the file.
                if (!entriesCreated.add(entryName)) {
                    logger.debug("Skipping entry {} in {} because an entry with that name already exists", entryName, workingDirectory);
                    continue;
                }

                // Add a jar entry to the output JAR file and copy the contents of the file
                final JarEntry outEntry = new JarEntry(jarEntry.getName());
                out.putNextEntry(outEntry);

                if (!jarEntry.isDirectory()) {
                    copy(jarInputStream, out);
                }

                // Ensure that we close the entry.
                out.closeEntry();
            }
        }
    }

    private static void copy(final InputStream in, final OutputStream out) throws IOException {
        byte[] buffer = new byte[4096];
        int len;
        while ((len = in.read(buffer)) > 0) {
            out.write(buffer, 0, len);
        }
    }

    private static void determineNiFiComponents(final BundleCoordinate coordinate, final File jar, final ExtensionMapping extensionMapping) throws IOException {
        final ExtensionMapping jarExtensionMapping = getJarExtensionMapping(coordinate, jar);

        // skip if there are no components
        if (jarExtensionMapping.isEmpty()) {
            return;
        }

        // merge the extension mapping found in this jar
        extensionMapping.merge(jarExtensionMapping);
    }

    private static ExtensionMapping getJarExtensionMapping(final BundleCoordinate coordinate, final File jar) throws IOException {
        final ExtensionMapping mapping = new ExtensionMapping();

        try (final JarFile jarFile = new JarFile(jar)) {
            final JarEntry processorEntry = jarFile.getJarEntry("META-INF/services/org.apache.nifi.processor.Processor");
            final JarEntry reportingTaskEntry = jarFile.getJarEntry("META-INF/services/org.apache.nifi.reporting.ReportingTask");
            final JarEntry flowAnalysisRuleEntry = jarFile.getJarEntry("META-INF/services/org.apache.nifi.flowanalysis.FlowAnalysisRule");
            final JarEntry controllerServiceEntry = jarFile.getJarEntry("META-INF/services/org.apache.nifi.controller.ControllerService");
            final JarEntry parameterProviderEntry = jarFile.getJarEntry("META-INF/services/org.apache.nifi.parameter.ParameterProvider");
            final JarEntry flowRegistryClientEntry = jarFile.getJarEntry("META-INF/services/org.apache.nifi.registry.flow.FlowRegistryClient");

            if (processorEntry == null && reportingTaskEntry == null && flowAnalysisRuleEntry == null && controllerServiceEntry == null && parameterProviderEntry == null) {
                return mapping;
            }

            mapping.addAllProcessors(coordinate, detectNiFiComponents(jarFile, processorEntry));
            mapping.addAllReportingTasks(coordinate, detectNiFiComponents(jarFile, reportingTaskEntry));
            mapping.addAllFlowAnalysisRules(coordinate, detectNiFiComponents(jarFile, flowAnalysisRuleEntry));
            mapping.addAllControllerServices(coordinate, detectNiFiComponents(jarFile, controllerServiceEntry));
            mapping.addAllParameterProviders(coordinate, detectNiFiComponents(jarFile, parameterProviderEntry));
            mapping.addAllFlowRegistryClients(coordinate, detectNiFiComponents(jarFile, flowRegistryClientEntry));
            return mapping;
        }
    }

    private static List<String> detectNiFiComponents(final JarFile jarFile, final JarEntry jarEntry) throws IOException {
        final List<String> componentNames = new ArrayList<>();

        if (jarEntry == null) {
            return componentNames;
        }

        try (final InputStream entryInputStream = jarFile.getInputStream(jarEntry);
                final BufferedReader reader = new BufferedReader(new InputStreamReader(entryInputStream))) {

            String line;
            while ((line = reader.readLine()) != null) {
                final String trimmedLine = line.trim();
                if (!trimmedLine.isEmpty() && !trimmedLine.startsWith("#")) {
                    final int indexOfPound = trimmedLine.indexOf("#");
                    final String effectiveLine = (indexOfPound > 0) ? trimmedLine.substring(0, indexOfPound) : trimmedLine;
                    componentNames.add(effectiveLine);
                }
            }
        }

        return componentNames;
    }

    /**
     * Creates the specified file, whose contents will come from the
     * <tt>InputStream</tt>.
     *
     * @param inputStream
     *            the contents of the file to create.
     * @param file
     *            the file to create.
     * @throws IOException
     *             if the file could not be created.
     */
    private static void makeFile(final InputStream inputStream, final File file) throws IOException {
        // Ensure parent directories exist to handle archives where directory entries
        // appear after file entries (e.g., after jarsigner has reordered entries)
        final File parent = file.getParentFile();
        if (parent != null) {
            Files.createDirectories(parent.toPath());
        }
        try (final InputStream in = inputStream;
                final FileOutputStream fos = new FileOutputStream(file)) {
            byte[] bytes = new byte[65536];
            int numRead;
            while ((numRead = in.read(bytes)) != -1) {
                fos.write(bytes, 0, numRead);
            }
        }
    }

    private static File getMappedJarEntryFile(final File workingDirectory, final JarEntry jarEntry) {
        final String jarEntryName = jarEntry.getName().replace(BUNDLED_DEPENDENCIES_PREFIX, BUNDLED_DEPENDENCIES_DIRECTORY);
        return getJarEntryFile(workingDirectory, jarEntryName);
    }

    private static File getJarEntryFile(final File workingDirectory, final String jarEntryName) {
        final Path workingDirectoryPath = workingDirectory.toPath().normalize();
        final Path jarEntryPath = workingDirectoryPath.resolve(jarEntryName).normalize();
        if (jarEntryPath.startsWith(workingDirectoryPath)) {
            return jarEntryPath.toFile();
        }
        throw new IllegalArgumentException(String.format("NAR Entry path not valid [%s]", jarEntryName));
    }

    private NarUnpacker() {
    }
}
