/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.nar;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.util.FileUtils;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Standard implementation of {@link NarPersistenceProvider} that stores NARs in a directory on the local filesystem.
 */
public class StandardNarPersistenceProvider implements NarPersistenceProvider {

    private static final Logger logger = LoggerFactory.getLogger(StandardNarPersistenceProvider.class);

    private static final String STORAGE_LOCATION_PROPERTY = "directory";
    private static final String DEFAULT_STORAGE_LOCATION = "./nar_repository";

    private static final String TEMP_STORAGE_DIR = "temp";
    private static final String INSTALLED_STORAGE_DIR = "installed";

    private static final String NAR_PATH_FORMAT = "%s/%s/%s";
    private static final String NAR_FILENAME_EXTENSION = ".nar";
    private static final String NAR_FILENAME_FORMAT = "%s-%s" + NAR_FILENAME_EXTENSION;
    private static final String NAR_PROPERTIES_FILE_EXTENSION = ".properties";
    private static final String NAR_PROPERTIES_FILE_FORMAT = "%s-%s" + NAR_PROPERTIES_FILE_EXTENSION;

    private volatile File storageLocation;
    private volatile File tempStorageLocation;
    private volatile File installedStorageLocation;
    private final Map<BundleCoordinate, NarPersistenceInfo> narPersistenceInfoMap = new ConcurrentHashMap<>();

    @Override
    public void initialize(final NarPersistenceProviderInitializationContext initializationContext) throws IOException {
        final String storageLocationPropertyValue = getStorageLocation(initializationContext);
        storageLocation = new File(storageLocationPropertyValue);
        try {
            FileUtils.ensureDirectoryExistAndCanReadAndWrite(storageLocation);
        } catch (final IOException e) {
            throw new RuntimeException("The NAR Persistence Provider's [" + STORAGE_LOCATION_PROPERTY + "] property is set to [" + storageLocationPropertyValue
                    + "] but the directory does not exist and cannot be created", e);
        }

        tempStorageLocation = new File(storageLocation, TEMP_STORAGE_DIR);
        if (!tempStorageLocation.exists() && !tempStorageLocation.mkdir()) {
            throw new RuntimeException("Unable to create temp storage location at [" + tempStorageLocation.getAbsolutePath() + "]");
        }

        installedStorageLocation = new File(storageLocation, INSTALLED_STORAGE_DIR);
        if (!installedStorageLocation.exists() && !installedStorageLocation.mkdir()) {
            throw new RuntimeException("Unable to create installed storage location at [" + installedStorageLocation.getAbsolutePath() + "]");
        }

        final Set<NarPersistenceInfo> narPersistenceInfos = loadNarPersistenceInfo();
        logger.info("Loaded persistence info for {} NARs", narPersistenceInfos.size());

        narPersistenceInfoMap.clear();
        narPersistenceInfos.forEach(narPersistenceInfo -> {
            final BundleCoordinate narCoordinate = narPersistenceInfo.getNarProperties().getCoordinate();
            narPersistenceInfoMap.put(narCoordinate, narPersistenceInfo);
        });

        logger.info("Initialization completed with storage location [{}]", storageLocation.getAbsolutePath());
    }

    @Override
    public File createTempFile(final InputStream inputStream) throws IOException {
        final File tempFile = getTempFile();
        try {
            Files.copy(inputStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            return tempFile;
        } catch (final IOException e) {
            if (tempFile.exists() && !tempFile.delete()) {
                logger.warn("Failed to delete temp NAR file [{}], this file should be cleaned up manually", tempFile.getAbsolutePath());
            }
            throw new IOException("Failed to write NAR to temp file at [" + tempFile.getAbsolutePath() + "]", e);
        }
    }

    @Override
    public NarPersistenceInfo saveNar(final NarPersistenceContext persistenceContext, final File tempNarFile) throws IOException {
        final NarManifest manifest = persistenceContext.getManifest();
        final BundleCoordinate coordinate = manifest.getCoordinate();

        final File narVersionDirectory = getNarVersionDirectory(coordinate);
        if (!narVersionDirectory.exists() && !narVersionDirectory.mkdirs()) {
            throw new IOException("Unable to create NAR directories: " + narVersionDirectory.getAbsolutePath());
        }

        final File narFile = getNarFile(narVersionDirectory, coordinate);
        try {
            Files.move(tempNarFile.toPath(), narFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            logger.debug("Saved NAR to [{}]", narFile.getAbsolutePath());
        } catch (final IOException e) {
            throw new IOException("Failed to move NAR from [" + tempNarFile.getAbsolutePath() + "] to [" + narFile.getAbsolutePath() + "]", e);
        }

        final NarProperties narProperties = createNarProperties(persistenceContext);

        final File narPropertiesFile = getNarProperties(narVersionDirectory, coordinate);
        try (final OutputStream outputStream = new FileOutputStream(narPropertiesFile)) {
            final Properties properties = narProperties.toProperties();
            properties.store(outputStream, null);
            logger.debug("Saved NAR Properties to [{}]", narPropertiesFile.getAbsolutePath());
        } catch (final IOException e) {
            throw new IOException("Failed to create NAR properties file for [" + coordinate + "]", e);
        }

        final NarPersistenceInfo narPersistenceInfo = new NarPersistenceInfo(narFile, narProperties);
        narPersistenceInfoMap.put(narPersistenceInfo.getNarProperties().getCoordinate(), narPersistenceInfo);
        return narPersistenceInfo;
    }

    @Override
    public void deleteNar(final BundleCoordinate narCoordinate) throws IOException {
        narPersistenceInfoMap.remove(narCoordinate);

        final File narVersionDirectory = getNarVersionDirectory(narCoordinate);
        final File narPropertiesFile = getNarProperties(narVersionDirectory, narCoordinate);
        final File narFile = getNarFile(narVersionDirectory, narCoordinate);

        if (narPropertiesFile.exists()) {
            try {
                Files.delete(narPropertiesFile.toPath());
                logger.info("Deleted NAR Properties [{}]", narPropertiesFile.getAbsolutePath());
            } catch (final IOException e) {
                throw new IOException("Failed to delete NAR Properties [" + narPropertiesFile.getAbsolutePath() + "]", e);
            }
        }

        if (narFile.exists()) {
            try {
                Files.delete(narFile.toPath());
                logger.info("Deleted NAR [{}]", narFile.getAbsolutePath());
            } catch (final IOException e) {
                throw new IOException("Failed to delete NAR [" + narFile.getAbsolutePath() + "]", e);
            }
        }

        if (narVersionDirectory.exists()) {
            try {
                Files.delete(narVersionDirectory.toPath());
                logger.info("Deleted NAR directory [{}]", narVersionDirectory.getAbsolutePath());
            } catch (final IOException e) {
                throw new IOException("Failed to delete NAR directory [" + narVersionDirectory.getAbsolutePath() + "]", e);
            }
        }
    }

    @Override
    public InputStream readNar(final BundleCoordinate narCoordinate) throws FileNotFoundException {
        final File narVersionDirectory = getNarVersionDirectory(narCoordinate);
        final File narFile = getNarFile(narVersionDirectory, narCoordinate);
        if (!narFile.exists()) {
            throw new FileNotFoundException("NAR file [" + narFile.getAbsolutePath() + "] does not exist");
        }
        return new FileInputStream(narFile);
    }

    @Override
    public NarPersistenceInfo getNarInfo(final BundleCoordinate narCoordinate) throws IOException {
        final File narVersionDirectory = getNarVersionDirectory(narCoordinate);
        final File narPropertiesFile = getNarProperties(narVersionDirectory, narCoordinate);
        if (!narPropertiesFile.exists()) {
            throw new FileNotFoundException("NAR Properties file [" + narPropertiesFile.getAbsolutePath() + "] does not exist");
        }

        final NarProperties narProperties = readNarProperties(narPropertiesFile);
        final File narFile = getNarFile(narVersionDirectory, narCoordinate);
        return new NarPersistenceInfo(narFile, narProperties);
    }

    @Override
    public boolean exists(final BundleCoordinate narCoordinate) {
        return narPersistenceInfoMap.containsKey(narCoordinate);
    }

    @Override
    public Set<NarPersistenceInfo> getAllNarInfo() {
        return new HashSet<>(narPersistenceInfoMap.values());
    }

    @Override
    public void shutdown() {

    }

    private File getNarVersionDirectory(final BundleCoordinate coordinate) {
        final String path = NAR_PATH_FORMAT.formatted(coordinate.getGroup(), coordinate.getId(), coordinate.getVersion());
        return new File(installedStorageLocation, path);
    }

    private File getNarFile(final File directory, final BundleCoordinate coordinate) {
        final String filename = NAR_FILENAME_FORMAT.formatted(coordinate.getId(), coordinate.getVersion());
        return new File(directory, filename);
    }

    private File getNarProperties(final File directory, final BundleCoordinate coordinate) {
        final String filename = NAR_PROPERTIES_FILE_FORMAT.formatted(coordinate.getId(), coordinate.getVersion());
        return new File(directory, filename);
    }

    private File getTempFile() {
        return new File(tempStorageLocation, UUID.randomUUID().toString());
    }

    private String getStorageLocation(final NarPersistenceProviderInitializationContext initializationContext) {
        final Map<String, String> properties = initializationContext.getProperties();
        final String propertyValue = properties.get(STORAGE_LOCATION_PROPERTY);
        return StringUtils.isBlank(propertyValue) ? DEFAULT_STORAGE_LOCATION : propertyValue;
    }

    private NarProperties createNarProperties(final NarPersistenceContext persistenceContext) {
        final NarManifest narManifest = persistenceContext.getManifest();

        return NarProperties.builder()
                .sourceType(persistenceContext.getSource().name())
                .sourceId(persistenceContext.getSourceIdentifier())
                .narGroup(narManifest.getGroup())
                .narId(narManifest.getId())
                .narVersion(narManifest.getVersion())
                .narDependencyGroup(narManifest.getDependencyGroup())
                .narDependencyId(narManifest.getDependencyId())
                .narDependencyVersion(narManifest.getDependencyVersion())
                .installed(Instant.now())
                .build();
    }

    public Set<NarPersistenceInfo> loadNarPersistenceInfo() throws IOException {
        try (final Stream<Path> pathStream = Files.walk(installedStorageLocation.toPath())) {
            final List<File> narPropertyFiles = pathStream.filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .filter(file -> file.getName().endsWith(NAR_PROPERTIES_FILE_EXTENSION))
                    .toList();

            final Set<NarPersistenceInfo> narPersistenceInfos = new HashSet<>();
            for (final File narPropertiesFile : narPropertyFiles) {
                try {
                    final NarPersistenceInfo narPersistenceInfo = loadNarPersistenceInfo(narPropertiesFile);
                    narPersistenceInfos.add(narPersistenceInfo);
                } catch (final Exception e) {
                    logger.warn("Failed to load persistence info for NAR Properties [{}]", narPropertiesFile.getAbsolutePath(), e);
                }
            }
            return narPersistenceInfos;
        }
    }

    private NarPersistenceInfo loadNarPersistenceInfo(final File narPropertiesFile) throws IOException {
        logger.debug("Loading persistence info from NAR Properties [{}]", narPropertiesFile.getAbsolutePath());

        final NarProperties narProperties = readNarProperties(narPropertiesFile);
        final BundleCoordinate narCoordinate = narProperties.getCoordinate();
        logger.debug("Loaded NAR Properties for [{}]", narCoordinate);

        final File narVersionDirectory = getNarVersionDirectory(narCoordinate);
        final File narFile = getNarFile(narVersionDirectory, narCoordinate);
        return new NarPersistenceInfo(narFile, narProperties);
    }

    private NarProperties readNarProperties(final File narPropertiesFile) throws IOException {
        try (final InputStream inputStream = new FileInputStream(narPropertiesFile)) {
            return NarProperties.parse(inputStream);
        }
    }

}
