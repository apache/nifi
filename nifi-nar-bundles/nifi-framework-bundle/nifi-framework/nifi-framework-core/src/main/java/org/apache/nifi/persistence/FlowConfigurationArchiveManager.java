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
package org.apache.nifi.persistence;

import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Calendar;
import java.util.Comparator;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FlowConfigurationArchiveManager {

    private static final Logger logger = LoggerFactory.getLogger(FlowConfigurationArchiveManager.class);

    /**
     * Represents archive file name such as followings:
     * <li>yyyyMMddTHHmmss+HHmm_original-file-name</li>
     * <li>yyyyMMddTHHmmss-HHmm_original-file-name</li>
     * <li>yyyyMMddTHHmmssZ_original-file-name</li>
     */
    private final Pattern archiveFilenamePattern = Pattern.compile("^([\\d]{8}T[\\d]{6}([\\+\\-][\\d]{4}|Z))_.+$");
    private final Path flowConfigFile;
    private final Path archiveDir;
    private final Integer maxCount;
    private final Long maxTimeMillis;
    private final Long maxStorageBytes;

    public FlowConfigurationArchiveManager(final Path flowConfigFile, final NiFiProperties properties) {
        final String archiveDirVal = properties.getFlowConfigurationArchiveDir();
        final Path archiveDir = (archiveDirVal == null || archiveDirVal.equals(""))
                ? flowConfigFile.getParent().resolve("archive") : new File(archiveDirVal).toPath();

        this.maxCount = properties.getFlowConfigurationArchiveMaxCount();

        String maxTime = properties.getFlowConfigurationArchiveMaxTime();
        String maxStorage = properties.getFlowConfigurationArchiveMaxStorage();

        if (maxCount == null && StringUtils.isBlank(maxTime) && StringUtils.isBlank(maxStorage)) {
            // None of limitation is specified, fall back to the default configuration;
            maxTime = NiFiProperties.DEFAULT_FLOW_CONFIGURATION_ARCHIVE_MAX_TIME;
            maxStorage = NiFiProperties.DEFAULT_FLOW_CONFIGURATION_ARCHIVE_MAX_STORAGE;
            logger.info("None of archive max limitation is specified, fall back to the default configuration, maxTime={}, maxStorage={}",
                    maxTime, maxStorage);
        }

        this.maxTimeMillis = StringUtils.isBlank(maxTime) ? null : FormatUtils.getTimeDuration(maxTime, TimeUnit.MILLISECONDS);
        this.maxStorageBytes = StringUtils.isBlank(maxStorage) ? null : DataUnit.parseDataSize(maxStorage, DataUnit.B).longValue();

        this.flowConfigFile = flowConfigFile;
        this.archiveDir = archiveDir;
    }

    private String createArchiveFileName(final String originalFlowConfigFileName) {
        TimeZone tz = TimeZone.getDefault();
        Calendar cal = GregorianCalendar.getInstance(tz);
        int offsetInMillis = tz.getOffset(cal.getTimeInMillis());
        final int year = cal.get(Calendar.YEAR);
        final int month = cal.get(Calendar.MONTH) + 1;
        final int day = cal.get(Calendar.DAY_OF_MONTH);
        final int hour = cal.get(Calendar.HOUR_OF_DAY);
        final int min = cal.get(Calendar.MINUTE);
        final int sec = cal.get(Calendar.SECOND);

        String offset = String.format("%s%02d%02d",
                (offsetInMillis >= 0 ? "+" : "-"),
                Math.abs(offsetInMillis / 3600000),
                Math.abs((offsetInMillis / 60000) % 60));

        return String.format("%d%02d%02dT%02d%02d%02d%s_%s",
                year, month, day, hour, min, sec, offset, originalFlowConfigFileName);
    }

    /**
     * Setup a file to archive data flow. Create archive directory if it doesn't exist yet.
     * @return Resolved archive file which is ready to write to
     * @throws IOException when it fails to access archive dir
     */
    public File setupArchiveFile() throws IOException {
        Files.createDirectories(archiveDir);

        if (!Files.isDirectory(archiveDir)) {
            throw new IOException("Archive directory doesn't appear to be a directory " + archiveDir);
        }
        final String originalFlowConfigFileName = flowConfigFile.getFileName().toString();
        final Path archiveFile = archiveDir.resolve(createArchiveFileName(originalFlowConfigFileName));
        return archiveFile.toFile();
    }

    /**
     * Archive current flow configuration file by copying the original file to the archive directory.
     * Before creating new archive file, this method removes old archives to satisfy following conditions:
     * <ul>
     * <li>Number of archive files less than or equal to maxCount</li>
     * <li>Keep archive files which has last modified timestamp no older than current system timestamp - maxTimeMillis</li>
     * <li>Total size of archive files less than or equal to maxStorageBytes</li>
     * </ul>
     * This method keeps other files intact, so that users can keep particular archive by copying it with different name.
     * Whether a given file is archive file or not is determined by the filename.
     * Since archive file name consists of timestamp up to seconds, if archive is called multiple times within a second,
     * it will overwrite existing archive file with the same name.
     * @return Newly created archive file, archive filename is computed by adding ISO8601
     * timestamp prefix to the original filename, ex) 20160706T160719+0900_flow.xml.gz
     * @throws IOException If it fails to create new archive file.
     * Although, other IOExceptions like the ones thrown during removing expired archive files will not be thrown.
     */
    public File archive() throws IOException {
        final String originalFlowConfigFileName = flowConfigFile.getFileName().toString();

        final File archiveFile = setupArchiveFile();

        // Collect archive files by its name
        final long now = System.currentTimeMillis();
        final AtomicLong totalArchiveSize = new AtomicLong(0);
        final List<Path> archives = Files.walk(archiveDir, 1).filter(p -> {
            final String filename = p.getFileName().toString();
            if (Files.isRegularFile(p) && filename.endsWith("_" + originalFlowConfigFileName)) {
                final Matcher matcher = archiveFilenamePattern.matcher(filename);
                if (matcher.matches() && filename.equals(matcher.group(1) + "_" + originalFlowConfigFileName)) {
                    try {
                        totalArchiveSize.getAndAdd(Files.size(p));
                    } catch (IOException e) {
                        logger.warn("Failed to get file size of {} due to {}", p, e);
                    }
                    return true;
                }
            }
            return false;
        }).collect(Collectors.toList());

        // Sort by timestamp.
        archives.sort(Comparator.comparingLong(path -> path.toFile().lastModified()));

        logger.debug("archives={}", archives);

        final int archiveCount = archives.size();
        final long flowConfigFileSize = Files.size(flowConfigFile);
        IntStream.range(0, archiveCount).filter(i -> {
            // If maxCount is specified, remove old archives
            boolean old = maxCount != null && maxCount > 0 && (archiveCount - i) > maxCount - 1;
            // If maxTime is specified, remove expired archives
            final File archive = archives.get(i).toFile();
            old = old || (maxTimeMillis != null && maxTimeMillis > 0 && (now - archive.lastModified()) > maxTimeMillis);
            // If maxStorage is specified, remove old archives
            old = old || (maxStorageBytes != null && maxStorageBytes > 0 && (totalArchiveSize.get() + flowConfigFileSize > maxStorageBytes));

            if (old) {
                totalArchiveSize.getAndAdd(archive.length() * -1);
                logger.info("Removing old archive file {} to reduce storage usage. currentSize={}", archive, totalArchiveSize);
            }
            return old;
        }).forEach(i -> {
            try {
                Files.delete(archives.get(i));
            } catch (IOException e) {
                logger.warn("Failed to delete {} to reduce storage usage, due to {}", archives.get(i), e);
            }
        });

        // Create new archive file.
        Files.copy(flowConfigFile, archiveFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

        if (maxStorageBytes != null && maxStorageBytes > 0 && flowConfigFileSize > maxStorageBytes) {
            logger.warn("Size of {} ({}) exceeds configured maxStorage size ({}). Archive won't be able to keep old files.",
                    flowConfigFile, flowConfigFileSize, maxStorageBytes);
        }

        return archiveFile;
    }
}
