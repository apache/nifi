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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FlowConfigurationArchiveManager {

    private static final Logger logger = LoggerFactory.getLogger(FlowConfigurationArchiveManager.class);

    /**
     * Represents archive file name such as followings:
     * <li>yyyyMMddTHHmmss+HHmm_original-file-name</li>
     * <li>yyyyMMddTHHmmss-HHmm_original-file-name</li>
     * <li>yyyyMMddTHHmmssZ_original-file-name</li>
     */
    private final Pattern archiveFilenamePattern = Pattern.compile("^([\\d]{8}T[\\d]{6}([\\+\\-][\\d]{4}|Z))_.+$");
    private final Path flowFile;
    private final Path archiveDir;
    private final long maxTimeMillis;
    private final long maxStorageBytes;

    public FlowConfigurationArchiveManager(final Path flowFile, NiFiProperties properties) {
        final String archiveDirVal = properties.getFlowConfigurationArchiveDir();
        final Path archiveDir = (archiveDirVal == null || archiveDirVal.equals(""))
                ? flowFile.getParent().resolve("archive") : new File(archiveDirVal).toPath();

        final long archiveMaxTime =
                FormatUtils.getTimeDuration(properties.getFlowConfigurationArchiveMaxTime(), TimeUnit.MILLISECONDS);
        final long archiveMaxStorage =
                DataUnit.parseDataSize(properties.getFlowConfigurationArchiveMaxStorage(), DataUnit.B).longValue();

        this.flowFile = flowFile;
        this.archiveDir = archiveDir;
        this.maxTimeMillis = archiveMaxTime;
        this.maxStorageBytes = archiveMaxStorage;
    }

    public FlowConfigurationArchiveManager(final Path flowFile, final Path archiveDir, long maxTimeMillis, long maxStorageBytes) {
        this.flowFile = flowFile;
        this.archiveDir = archiveDir;
        this.maxTimeMillis = maxTimeMillis;
        this.maxStorageBytes = maxStorageBytes;
    }

    private String createArchiveFileName(final String originalFlowFileName) {
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
                year, month, day, hour, min, sec, offset, originalFlowFileName);
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
        final String originalFlowFileName = flowFile.getFileName().toString();
        final Path archiveFile = archiveDir.resolve(createArchiveFileName(originalFlowFileName));
        return archiveFile.toFile();
    }

    /**
     * Archive current flow configuration file by copying the original file to the archive directory.
     * After creating new archive file:
     * <li>It removes expired archive files based on its last modification date and maxTimeMillis</li>
     * <li>It removes old files until total size of archive files becomes less than maxStorageBytes</li>
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
        final String originalFlowFileName = flowFile.getFileName().toString();
        final File archiveFile = setupArchiveFile();
        Files.copy(flowFile, archiveFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

        // Collect archive files by its name, and group by expiry state.
        final long now = System.currentTimeMillis();
        final Map<Boolean, List<Path>> oldArchives = Files.walk(archiveDir, 1).filter(p -> {
            final String filename = p.getFileName().toString();
            if (Files.isRegularFile(p) && filename.endsWith("_" + originalFlowFileName)) {
                final Matcher matcher = archiveFilenamePattern.matcher(filename);
                if (matcher.matches() && filename.equals(matcher.group(1) + "_" + originalFlowFileName)) {
                    return true;
                }
            }
            return false;
        }).collect(Collectors.groupingBy(p -> (now - p.toFile().lastModified()) > maxTimeMillis, Collectors.toList()));

        logger.debug("oldArchives={}", oldArchives);

        // Remove expired files
        final List<Path> expiredArchives = oldArchives.get(true);
        if (expiredArchives != null) {
            expiredArchives.stream().forEach(p -> {
                try {
                    logger.info("Removing expired archive file {}", p);
                    Files.delete(p);
                } catch (IOException e) {
                    logger.warn("Failed to delete expired archive {} due to {}", p, e.toString());
                }
            });
        }

        // Calculate size
        final List<Path> remainingArchives = oldArchives.get(false);
        final long totalArchiveSize = remainingArchives.stream().mapToLong(p -> {
            try {
                return Files.size(p);
            } catch (IOException e) {
                logger.warn("Failed to get file size of {} due to {}", p, e.toString());
                return 0;
            }
        }).sum();
        logger.debug("totalArchiveSize={}", totalArchiveSize);

        // Remove old files until total size gets less than max storage size
        remainingArchives.sort((a, b)
                -> Long.valueOf(a.toFile().lastModified()).compareTo(Long.valueOf(b.toFile().lastModified())));
        long reducedTotalArchiveSize = totalArchiveSize;
        for (int i = 0; i < remainingArchives.size()
                && reducedTotalArchiveSize > maxStorageBytes; i++) {
            final Path path = remainingArchives.get(i);
            try {
                logger.info("Removing archive file {} to reduce storage usage. currentSize={}", path, reducedTotalArchiveSize);
                final long size = Files.size(path);
                Files.delete(path);
                reducedTotalArchiveSize -= size;
            } catch (IOException e) {
                logger.warn("Failed to delete {} to reduce storage usage, due to {}", path, e.toString());
            }
        }

        return archiveFile;
    }
}
