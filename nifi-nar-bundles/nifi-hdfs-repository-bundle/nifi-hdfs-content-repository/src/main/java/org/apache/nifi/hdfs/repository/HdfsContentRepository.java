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
package org.apache.nifi.hdfs.repository;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.nifi.controller.repository.ContentNotFoundException;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.stream.io.SynchronizedByteCountingOutputStream;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores flow file content using the hadoop FileSystem API.
 *
 * In addition to the normal content repository properties, this one takes the following additional properties:
 *
 * Required:
 *
 * nifi.content.repository.hdfs.core.site - the default hadoop core-site.xml file to configure file systems with.
 * Note: this isn't actually required as long as each container specifies its own core.site.xml. See below.
 *
 * Optional:
 * nifi.content.repository.hdfs.operating.mode - A comma separated list of operating modes that governs the
 * behavior of the content repository. See the 'OperatingMode' enum for possible values and their behavior.
 *
 * Each configured container can optionally specify their own core-site.xml.
 * Example:
 *
 * Assume the following two containers:
 * nifi.content.repository.directory.location1=uri://path/to/dir1
 * nifi.content.repository.directory.location2=uri://path/to/dir2
 *
 * Then the following two properites can also be provided:
 * nifi.content.repository.hdfs.core.site.location1=/path/to/core-site-1.xml
 * nifi.content.repository.hdfs.core.site.location2=/path/to/core-site-2.xml
 *
 * nifi.content.repository.hdfs.full.percentage - the percentage ('##%') of a container's
 * capcity that must be occupied before treating the container as 'full'. Note: once a container is full,
 * all writes will stop for that container. If all containers are full and there is no fallback, claim creation will
 * stop until space becomes available. Note: if a value isn't specified explicitly, 95 is used.
 *
 * nifi.content.repository.hdfs.failure.timeout - the amount of time to wait when a
 * failure ocurrs for a container before attempting to use that container again for writing.
 *
 * nifi.content.repository.hdfs.wait.active.containers.timeout - the amount of time to wait
 * for an active container to be avaible before giving up and throwing an exception. Defaults to indefinite.
 */
public class HdfsContentRepository implements ClaimClosedHandler, ArchivableRepository, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsContentRepository.class);
    protected static final int HEALTH_CHECK_RUN_INTERVAL_SECONDS = 15;
    protected static final String CORE_SITE_DEFAULT_PROPERTY = "nifi.content.repository.hdfs.core.site";
    protected static final String OPERATING_MODE_PROPERTY = "nifi.content.repository.hdfs.operating.mode";
    protected static final String FULL_PERCENTAGE_PROPERTY = "nifi.content.repository.hdfs.full.percentage";
    protected static final String FAILURE_TIMEOUT_PROPERTY = "nifi.content.repository.hdfs.failure.timeout";
    protected static final String PRIMARY_GROUP_PROPERTY = "nifi.content.repository.hdfs.primary";
    protected static final String ARCHIVE_GROUP_PROPERTY = "nifi.content.repository.hdfs.archive";
    protected static final String WAIT_FOR_CONTAINERS_TIMEOUT = "nifi.content.repository.hdfs.wait.active.containers.timeout";
    protected static final String SECTIONS_PER_CONTAINER_PROPERTY = "nifi.content.repository.hdfs.sections.per.container";
    protected static final String ARCHIVE_DIR_NAME = "archive";
    protected static final Pattern MAX_ARCHIVE_SIZE_PATTERN = Pattern.compile("^[0-9][0-9]?%");
    private final AtomicLong nextClaimIndex = new AtomicLong();
    private final ConcurrentMap<ResourceClaim, ByteCountingOutputStream> writableClaimStreams = new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<ClaimAndLength> writableClaims;
    private final ContainerGroup primary;
    private final ContainerGroup secondary;
    private final ContainerGroup archive;
    private final Map<String, Container> allContainers;
    private final ScheduledExecutorService workThreads;
    private final RepositoryConfig repoConfig;
    private volatile ContainerGroup activeGroup;
    private volatile ResourceClaimManager claimManager = null;

    public HdfsContentRepository() {
        this.primary = null;
        this.secondary = null;
        this.archive = null;
        this.repoConfig = null;
        this.writableClaims = null;
        this.workThreads = null;
        this.allContainers = null;
    }

    public HdfsContentRepository(NiFiProperties properties) {
        // free executors created by FileSystemRepository

        this.repoConfig = new RepositoryConfig(properties);
        this.writableClaims = new LinkedBlockingQueue<>(repoConfig.getMaxFlowFilesPerClaim());

        Set<String> primaryIds = parseIds(properties, PRIMARY_GROUP_PROPERTY);
        Set<String> archiveIds = parseIds(properties, ARCHIVE_GROUP_PROPERTY);

        if (repoConfig.hasMode(OperatingMode.Archive)) {
            LOG.info("Using 'Archive' operating mode");
            this.archive = new ContainerGroup(properties, repoConfig, archiveIds, null);
        } else {
            archiveIds = Collections.emptySet();
            this.archive = null;
        }

        Set<String> excludeSecondaryIds = new HashSet<>(primaryIds);
        excludeSecondaryIds.addAll(archiveIds);

        if (repoConfig.hasMode(OperatingMode.CapacityFallback) || repoConfig.hasMode(OperatingMode.FailureFallback)) {
            OperatingMode fallback = repoConfig.hasMode(OperatingMode.CapacityFallback) ? OperatingMode.CapacityFallback : OperatingMode.FailureFallback;
            LOG.info("Using '" + fallback + "' fallback operating mode");
            this.primary = new ContainerGroup(properties, repoConfig, primaryIds, null);
            this.secondary = new ContainerGroup(properties, repoConfig, null, excludeSecondaryIds);
        } else {
            LOG.info("Using non-fallback (Normal) operating mode");
            this.primary = new ContainerGroup(properties, repoConfig, null, archiveIds);
            this.secondary = null;
        }

        Map<String, Container> allContainers = new HashMap<>(primary.getAll());
        if (secondary != null) {
            allContainers.putAll(secondary.getAll());
        }
        if (archive != null) {
            allContainers.putAll(archive.getAll());
        }

        this.activeGroup = primary;
        this.allContainers = allContainers;

        int numThreads = 2 + (2 * allContainers.size());
        if (archive != null) {
            numThreads -= archive.getNumContainers();

            // we need a re-archive thread for each primary/secondary container if rearchiving is enabled.
            numThreads += primary.getNumContainers();
            if (secondary != null) {
                numThreads += secondary.getNumContainers();
            }
        }

        this.workThreads = Executors.newScheduledThreadPool(numThreads, new NamedThreadFactory("HdfsContentRepository Worker"));
    }

    private Set<String> parseIds(NiFiProperties properties, String property) {
        String value = properties.getProperty(property, "");
        if (value.isEmpty()) {
            return Collections.emptySet();
        }
        return new HashSet<>(Arrays.asList(value.split(",")));
    }

    @Override
    public boolean isArchiveEnabled() {
        return repoConfig.isArchiveData();
    }

    public RepositoryConfig getConfig() {
        return repoConfig;
    }

    /** protected mostly for testing purposes */
    protected ConcurrentMap<ResourceClaim, ByteCountingOutputStream> getWritableClaimStreams() {
        return writableClaimStreams;
    }

    @Override
    public void initialize(ResourceClaimManager claimManager) {
        this.claimManager = claimManager;

        Map<String, Container> workContainers = new HashMap<>(primary.getAll());
        if (secondary != null) {
            workContainers.putAll(secondary.getAll());
        }

        workThreads.scheduleWithFixedDelay(new BinDestructableClaims(claimManager, workContainers), 1, 1, TimeUnit.SECONDS);

        workThreads.scheduleWithFixedDelay(new ContainerHealthMonitor(this, primary, secondary, repoConfig),
            HEALTH_CHECK_RUN_INTERVAL_SECONDS, HEALTH_CHECK_RUN_INTERVAL_SECONDS, TimeUnit.SECONDS);

        for (int i = 0; i < workContainers.size(); i++) {
            workThreads.scheduleWithFixedDelay(new ArchiveOrDestroyDestructableClaims(this, workContainers.values()), 1, 1, TimeUnit.SECONDS);
        }

        for (Container container : allContainers.values()) {
            workThreads.scheduleWithFixedDelay(new DestroyExpiredArchiveClaims(this, container, repoConfig.getMaxArchiveMillis()), 1, 1, TimeUnit.SECONDS);
        }

        if (archive != null) {
            for (Container container : primary) {
                workThreads.scheduleWithFixedDelay(new ReArchiveClaims(container, archive, repoConfig.getSectionsPerContainer()), 1, 1, TimeUnit.SECONDS);
            }
            if (secondary != null) {
                for (Container container : secondary) {
                    workThreads.scheduleWithFixedDelay(new ReArchiveClaims(container, archive, repoConfig.getSectionsPerContainer()), 1, 1, TimeUnit.SECONDS);
                }
            }
        }
    }

    protected void setActiveGroup(ContainerGroup activeGroup) {
        this.activeGroup = activeGroup;
    }

    @Override
    public Set<String> getContainerNames() {
        return new HashSet<>(allContainers.keySet());
    }

    @Override
    public long getContainerCapacity(String containerName) throws IOException {
        Container container = getContainer(containerName);

        FsStatus status = container.getFileSystem().getStatus(container.getPath());
        int replication = container.getConfig().getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
        return status.getCapacity() / replication;
    }

    @Override
    public long getContainerUsableSpace(String containerName) throws IOException {
        Container container = getContainer(containerName);

        FsStatus status = container.getFileSystem().getStatus(container.getPath());
        int replication = container.getConfig().getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
        return status.getRemaining() / replication;
    }

    @Override
    public String getContainerFileStoreName(String containerName) {
        return getContainer(containerName).getPath().getName();
    }

    private Container getContainer(ContentClaim claim) {
        return getContainer(claim.getResourceClaim().getContainer());
    }
    private Container getContainer(ResourceClaim claim) {
        return getContainer(claim.getContainer());
    }
    private Container getContainer(String name) {
        Container container = allContainers.get(name);

        if (container == null) {
            throw new IllegalArgumentException("Container doesn't exist with name: " + name);
        }

        return container;
    }

    /**
     * Adapted from FileSystemRepository
     *
     * Removed use of 'ContainerState' and backpressure due to the assumption
     * that it's not possible to fill up HDFS faster than it would be to age
     * archived files off.
     */
    @Override
    public ContentClaim create(boolean lossTolerant) throws IOException {
        ClaimAndLength reuse = writableClaims.poll();

        ResourceClaim claim;
        long offset;
        if (reuse == null) {
            long currentIndex = nextClaimIndex.incrementAndGet();

            Container container = getActiveContainer(currentIndex);

            long modulatedSectionIndex = currentIndex % repoConfig.getSectionsPerContainer();
            String section = String.valueOf(modulatedSectionIndex).intern();
            String claimId = System.currentTimeMillis() + "-" + currentIndex;

            claim = claimManager.newResourceClaim(container.getName(), section, claimId, lossTolerant, true);
            offset = 0L;
            LOG.debug("Creating new Resource Claim {}", claim);

            // we always append because there may be another ContentClaim using the same resource claim.
            // However, we know that we will never write to the same claim from two different threads
            // at the same time because we will call create() to get the claim before we write to it,
            // and when we call create(), it will remove it from the Queue, which means that no other
            // thread will get the same Claim until we've finished writing to it.
            Path file = container.createPath(claim);
            try {
                ByteCountingOutputStream claimStream = new SynchronizedByteCountingOutputStream(container.getFileSystem().create(file));
                writableClaimStreams.put(claim, claimStream);
                claimManager.incrementClaimantCount(claim, true);
            } catch (IOException ex) {
                container.failureOcurred();
                throw new IOException("Failed to create output stream for claim: " + claim, ex);
            }
        } else {
            claim = reuse.getClaim();
            offset = reuse.getLength();
            claimManager.incrementClaimantCount(reuse.getClaim(), false);
        }
        return new StandardContentClaim(claim, offset);
    }

    protected Container getActiveContainer(long currentIndex) throws IOException {
        Container container = activeGroup.nextActiveAtModIndex(currentIndex);
        if (container == null) {
            // wait for a container to become available
            Thread thread = Thread.currentThread();
            String origName = thread.getName();
            try {
                long started = System.currentTimeMillis();
                thread.setName(origName + " - waiting for active containers since " + started);
                while ((container = activeGroup.nextActiveAtModIndex(currentIndex)) == null
                    && System.currentTimeMillis() - started < repoConfig.getWaitForContainerTimeoutMs()) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ex) {
                        throw new IOException(ex);
                    }
                }
                if (container == null) {
                    throw new IOException("No containers are active to create a claim within");
                }
            } finally {
                thread.setName(origName);
            }
        }
        return container;
    }

    @Override
    public int incrementClaimaintCount(ContentClaim claim) {
        if (claim == null || claim.getResourceClaim() == null) {
            return 0;
        }

        return claimManager.incrementClaimantCount(claim.getResourceClaim(), false);
    }

    @Override
    public int getClaimantCount(final ContentClaim claim) {
        if (claim == null) {
            return 0;
        }

        return claimManager.getClaimantCount(claim.getResourceClaim());
    }

    @Override
    public int decrementClaimantCount(final ContentClaim claim) {
        if (claim == null) {
            return 0;
        }

        return claimManager.decrementClaimantCount(claim.getResourceClaim());
    }
    @Override
    public boolean remove(final ContentClaim claim) {
        if (claim == null) {
            return false;
        }

        return remove(claim.getResourceClaim());
    }

    /**
     * Adapted from FileSystemRepository
     */
    @Override
    public boolean remove(ResourceClaim claim) {
        if (claim == null) {
            return false;
        }

        // If the claim is still in use, we won't remove it.
        if (claim.isInUse()) {
            return false;
        }

        Container container;
        try {
            container = getContainer(claim);
        } catch (IllegalArgumentException ex) {
            LOG.warn("Unable to delete {}, unknown container: {}", claim, claim.getContainer());
            return false;
        }
        Path path = container.createPath(claim);

        // Ensure that we have no writable claim streams for this resource claim
        ByteCountingOutputStream stream = writableClaimStreams.remove(claim);

        if (stream != null) {
            try {
                stream.close();
            } catch (final IOException e) {
                LOG.warn("Failed to close Output Stream for {} due to {}", claim, e);
            }
        }

        try {
            FileSystem fs = container.getFileSystem();
            if (!fs.delete(path, false) && fs.exists(path)) {
                LOG.warn("Unable to delete {} at path {}", new Object[]{claim, path});
                return false;
            }
        } catch (IOException ex) {
            container.failureOcurred();
            LOG.error("Unable to delete " + claim + " at path " + path, ex);
            return false;
        }

        return true;
    }

    @Override
    public long size(final ContentClaim claim) throws IOException {
        if (claim == null) {
            return 0L;
        }

        // see javadocs for claim.getLength() as to why we do this.
        if (claim.getLength() < 0) {
            if (claim.getResourceClaim() == null) {
                return 0L;
            }

            ClaimStatus status = getStatus(claim, true);
            return status.getSize() - claim.getOffset();
        }

        return claim.getLength();
    }

    protected ClaimStatus getStatus(ContentClaim claim, boolean throwError) throws ContentNotFoundException, IOException {
        if (claim == null) {
            if (throwError) {
                throw new ContentNotFoundException(claim);
            } else {
                return null;
            }
        }

        ResourceClaim resourceClaim = claim.getResourceClaim();
        if (resourceClaim == null) {
            if (throwError) {
                throw new ContentNotFoundException(claim);
            } else {
                return null;
            }
        }

        Container container = getContainer(resourceClaim);
        FileSystem fs = container.getFileSystem();
        Path path = container.createPath(resourceClaim);

        try {
            FileStatus status = fs.getFileStatus(path);
            if (status != null) {
                return new ClaimStatus(status.getLen(), status.getPath(), container);
            }
        } catch (FileNotFoundException ex) { }

        if (archive != null) {
            String claimId = resourceClaim.getId();

            Container archiveContainer = archive.atModIndex(claimId.hashCode());
            FileSystem archiveFs = archiveContainer.getFileSystem();
            String section = path.getParent().getName();
            Path archivePath = new Path(archiveContainer.getPath(), section + "/" + ARCHIVE_DIR_NAME + "/" + claimId);
            try {
                FileStatus status = archiveFs.getFileStatus(archivePath);
                if (status != null) {
                    return new ClaimStatus(status.getLen(), archivePath, archiveContainer);
                }
            } catch (FileNotFoundException ex) { }
        }

        path = new Path(path.getParent(), ARCHIVE_DIR_NAME + "/" + path.getName());
        try {
            FileStatus status = fs.getFileStatus(path);
            if (status != null) {
                return new ClaimStatus(status.getLen(), status.getPath(), container);
            }
        } catch (FileNotFoundException ex) { }

        if (throwError) {
            throw new ContentNotFoundException(claim);
        } else {
            return null;
        }
    }

    @Override
    public Set<ResourceClaim> getActiveResourceClaims(String containerName) throws IOException {
        Container container = allContainers.get(containerName);
        if (container == null) {
            return Collections.emptySet();
        } else if (archive != null && archive.get(containerName) != null) {
            return Collections.emptySet();
        }

        FileSystem fs = container.getFileSystem();
        FileStatus[] sections;
        try {
            sections = fs.listStatus(container.getPath());
            if (sections == null || sections.length == 0) {
                return Collections.emptySet();
            }
        } catch (FileNotFoundException ex) {
            return Collections.emptySet();
        }

        Set<ResourceClaim> activeClaims = new HashSet<>();
        for (FileStatus section : sections) {
            if (!section.isDirectory()) {
                // not sure what this is if it's not a directory
                continue;
            }
            RemoteIterator<FileStatus> iterator = fs.listStatusIterator(section.getPath());
            if (iterator == null) {
                continue;
            }

            String sectionName = section.getPath().getName();
            while (iterator.hasNext()) {
                FileStatus status = iterator.next();
                if (!status.isFile()) {
                    continue;
                }

                String identifier = status.getPath().getName();

                ResourceClaim resourceClaim = claimManager.getResourceClaim(containerName, sectionName, identifier);
                if (resourceClaim == null) {
                    resourceClaim = claimManager.newResourceClaim(containerName, sectionName, identifier, false, false);
                }

                activeClaims.add(resourceClaim);
            }
        }

        LOG.debug("Obtaining active resource claims, will return a list of {} resource claims for container {}", activeClaims.size(), containerName);
        if (LOG.isTraceEnabled()) {
            LOG.trace("Listing of resource claims:");
            activeClaims.forEach(claim -> LOG.trace(claim.toString()));
        }

        return activeClaims;
    }

    @Override
    public InputStream read(ContentClaim claim) throws IOException {
        if (claim == null) {
            return new ByteArrayInputStream(new byte[0]);
        }

        long size = -1;
        Container container = getContainer(claim);
        FileSystem fs = container.getFileSystem();
        Path path = container.createPath(claim.getResourceClaim());
        FSDataInputStream inStream;
        try {
            inStream = fs.open(path);
        } catch (IOException ex) {
            try {
                // Assume the claim is archived and get it's actual location. We assume
                // it's not archived at first as an optimization since this is the common case.
                ClaimStatus status = getStatus(claim, true);
                container = status.getContainer();
                fs = container.getFileSystem();
                path = status.getPath();
                size = status.getSize();
                inStream = fs.open(path);
            } catch (IOException ex2) {
                container.failureOcurred();
                throw new IOException("Failed to open input stream for claim: " + claim, ex2);
            }
        }

        if (claim.getOffset() > 0L) {
            try {
                inStream.seek(claim.getOffset());
            } catch (IOException ex) {
                try {
                    inStream.close();
                } catch (Throwable ex2) { }

                // figure out whether or not the claim actually exists
                if (size < 0) {
                    try {
                        FileStatus status = fs.getFileStatus(path);
                        size = status.getLen();
                    } catch (FileNotFoundException ex2) {
                        // this shouldn't really happen
                    } catch (IOException ex2) { }
                }
                if (size >= 0 && claim.getOffset() >= size) {
                    throw new ContentNotFoundException(claim, "Content Claim has an offset of " + claim.getOffset() + " but Resource Claim " + path + " is only " + size + " bytes");
                } else {
                    container.failureOcurred();
                    throw new IOException("Could not seek to " + claim.getOffset() + " in claim " + path, ex);
                }
            }
        }

        // A claim length of -1 indicates that the claim is still being written to and we don't know
        // the length. In this case, we don't limit the Input Stream. If the Length has been populated, though,
        // it is possible that the Length could then be extended. However, we do want to avoid ever allowing the
        // stream to read past the end of the Content Claim. To accomplish this, we use a LimitedInputStream but
        // provide a LongSupplier for the length instead of a Long value. this allows us to continue reading until
        // we get to the end of the Claim, even if the Claim grows. This may happen, for instance, if we obtain an
        // InputStream for this claim, then read from it, write more to the claim, and then attempt to read again. In
        // such a case, since we have written to that same Claim, we should still be able to read those bytes.
        if (claim.getLength() >= 0) {
            return new LimitedInputStream(inStream, claim::getLength);
        } else {
            return inStream;
        }
    }

    @Override
    public OutputStream write(ContentClaim claim) throws IOException {
        StandardContentClaim scc = validateContentClaimForWriting(claim);

        ByteCountingOutputStream claimStream = writableClaimStreams.get(scc.getResourceClaim());

        OutputStream out = new ClaimOutputStream(this, scc, claimStream);

        LOG.debug("Writing to {}", out);
        if (LOG.isTraceEnabled()) {
            LOG.trace("Stack trace: ", new RuntimeException("Stack Trace for writing to " + out));
        }

        return out;
    }

    public static StandardContentClaim validateContentClaimForWriting(ContentClaim claim) {
        if (claim == null) {
            throw new NullPointerException("ContentClaim cannot be null");
        }

        if (!(claim instanceof StandardContentClaim)) {
            // we know that we will only create Content Claims that are of type StandardContentClaim, so if we get anything
            // else, just throw an Exception because it is not valid for this Repository
            throw new IllegalArgumentException("Cannot write to " + claim + " because that Content Claim does belong to this Content Repository");
        }

        if (claim.getLength() > 0) {
            throw new IllegalArgumentException("Cannot write to " + claim + " because it has already been written to.");
        }

        return (StandardContentClaim) claim;
    }

    @Override
    public ContentClaim clone(ContentClaim original, boolean lossTolerant) throws IOException {
        if (original == null) {
            return null;
        }

        ContentClaim newClaim = create(lossTolerant);

        Container oldContainer = getContainer(original);
        Container newContainer = getContainer(newClaim);

        Path oldPath = oldContainer.createPath(original.getResourceClaim());
        Path newPath = newContainer.createPath(newClaim.getResourceClaim());

        boolean readOkay = false;
        boolean writeOkay = false;

        try {
            FileSystem fs = oldContainer.getFileSystem();
            try {
                fs.concat(newPath, new Path[] { oldPath });
            } catch (UnsupportedOperationException ex) {
                try (InputStream in = read(original)) {
                    readOkay = true;
                    try (OutputStream out = write(newClaim)) {
                        writeOkay = true;
                        StreamUtils.copy(in, out);
                    }
                }
            }
        } catch (IOException ex) {
            if (readOkay && !writeOkay) {
                newContainer.failureOcurred();
            } else if (!readOkay) {
                oldContainer.failureOcurred();
            }
            decrementClaimantCount(newClaim);
            remove(newClaim);
            throw ex;
        }

        return newClaim;
    }

    @Override
    public void purge() {
        try {
            // delete all content from repositories
            for (Container container : allContainers.values()) {
                FileSystem fs = container.getFileSystem();
                if (!fs.exists(container.getPath())) {
                    continue;
                }

                RemoteIterator<LocatedFileStatus> fileIter;
                try {
                    fileIter = fs.listFiles(container.getPath(), true);
                } catch (FileNotFoundException ex) {
                    continue;
                }
                while (fileIter.hasNext()) {
                    LocatedFileStatus status = fileIter.next();
                    if (!status.isFile()) {
                        continue;
                    }
                    if (!fs.delete(status.getPath(), false)) {
                        LOG.warn("File appears to exist but unable to delete file: " + status.getPath());
                    }
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException("Purge failed", ex);
        }

        claimManager.purge();
    }

    @Override
    public void cleanup() {
        List<Container> cleanContainers = new ArrayList<>(primary.getAll().values());
        if (secondary != null) {
            cleanContainers.addAll(secondary.getAll().values());
        }
        for (Container container : cleanContainers) {
            try {
                FileSystem fs = container.getFileSystem();
                FileStatus[] sections;
                try {
                    sections = fs.listStatus(container.getPath());
                } catch (FileNotFoundException ex) {
                    continue;
                }

                for (FileStatus section : sections) {
                    if (!section.isDirectory()) {
                        continue;
                    }

                    String sectionName = section.getPath().getName();

                    try {
                        RemoteIterator<FileStatus> files = fs.listStatusIterator(section.getPath());
                        while (files.hasNext()) {
                            FileStatus file = files.next();
                            if (!file.isFile()) {
                                continue;
                            }

                            String id = file.getPath().getName();

                            ResourceClaim resourceClaim = claimManager.newResourceClaim(container.getName(), sectionName, id, false, false);
                            if (claimManager.getClaimantCount(resourceClaim) == 0) {
                                removeIncompleteContent(fs, container, file.getPath(), file.getLen());
                            }
                        }
                    } catch (IOException ex) {
                        container.failureOcurred();
                        LOG.error("Failed to run content repository cleanup for section " + section.getPath(), ex);
                    }
                }
            } catch (IOException ex) {
                container.failureOcurred();
                LOG.error("Failed to run content repository cleanup for container " + container.getName(), ex);
            }
        }
    }

    private void removeIncompleteContent(FileSystem fs, Container container, Path fileToRemove, long size) {
        String fileDescription = "" + fileToRemove + " (" + size + " bytes)";

        LOG.info("Found unknown file {} in content repository; {} file", fileDescription, repoConfig.isArchiveData() ? "archiving" : "removing");

        try {
            if (repoConfig.isArchiveData()) {
                archive(container, fileToRemove);
            } else {
                fs.delete(fileToRemove, false);
            }
        } catch (final IOException e) {
            final String action = repoConfig.isArchiveData() ? "archive" : "remove";
            LOG.warn("Unable to {} unknown file {} from content repository due to {}", action, fileDescription, e.toString());
            LOG.warn("", e);
        }
    }

    @Override
    public boolean archiveClaim(ResourceClaim claim) throws IOException {
        if (!repoConfig.isArchiveData()) {
            return false;
        }

        if (claim.isInUse()) {
            return false;
        }

        // If the claim count is decremented to 0 (<= 0 as a 'defensive programming' strategy), ensure that
        // we close the stream if there is one. There may be a stream open if create() is called and then
        // claimant count is removed without writing to the claim (or more specifically, without closing the
        // OutputStream that is returned when calling write() ).
        OutputStream out = writableClaimStreams.remove(claim);

        if (out != null) {
            try {
                out.close();
            } catch (IOException ex) {
                LOG.warn("Unable to close Output Stream for " + claim, ex);
            }
        }

        Container container = getContainer(claim);
        Path path = container.createPath(claim);
        boolean archived = archive(container, path);
        if (archived) {
            LOG.debug("Successfully moved {} to archive", claim);
        }
        return archived;
    }

    protected boolean archive(Container container, Path curPath) throws IOException {
        return archive(container.getFileSystem(), container, curPath);
    }

    protected boolean archive(FileSystem fs, Container container, Path curPath) throws IOException {
        // check if already archived
        final boolean alreadyArchived = ARCHIVE_DIR_NAME.equals(curPath.getParent().getName());
        if (alreadyArchived) {
            return false;
        }

        Path archivePath = new Path(curPath.getParent(), ARCHIVE_DIR_NAME + "/" + curPath.getName());
        return fs.rename(curPath, archivePath);
    }

    @Override
    public boolean isAccessible(ContentClaim contentClaim) throws IOException {
        ClaimStatus status = getStatus(contentClaim, false);
        return status != null;
    }

    @Override
    public long exportTo(ContentClaim claim, OutputStream destination) throws IOException {
        if (claim == null) {
            return 0L;
        }
        try (InputStream inStream = read(claim)) {
            return StreamUtils.copy(inStream, destination);
        }
    }

    @Override
    public long exportTo(ContentClaim claim, OutputStream destination, long offset, long length) throws IOException {
        if (offset < 0) {
            throw new IllegalArgumentException("offset cannot be negative");
        }
        final long claimSize = size(claim);
        if (offset > claimSize) {
            throw new IllegalArgumentException("offset of " + offset + " exceeds claim size of " + claimSize);
        }
        try (final InputStream inStream = read(claim)) {
            return IOUtils.copyLarge(inStream, destination, offset, length);
        }
    }

    @Override
    public long exportTo(ContentClaim claim, java.nio.file.Path destination, boolean append) throws IOException {
        if (claim == null) {
            return 0L;
        }

        File destFile = destination.toFile();
        try (InputStream inStream = read(claim); FileOutputStream outStream = new FileOutputStream(destFile, append)) {
            try {
                return StreamUtils.copy(inStream, outStream);
            } finally {
                if (repoConfig.isAlwaysSync()) {
                    outStream.getFD().sync();
                }
            }
        }
    }

    @Override
    public long exportTo(ContentClaim claim, java.nio.file.Path destination, boolean append, long offset, long length)
            throws IOException {
        if (claim == null) {
            return 0L;
        }

        File destFile = destination.toFile();
        try (InputStream inStream = read(claim); FileOutputStream outStream = new FileOutputStream(destFile, append)) {
            try {
                return IOUtils.copyLarge(inStream, outStream, offset, length);
            } finally {
                if (repoConfig.isAlwaysSync()) {
                    outStream.getFD().sync();
                }
            }
        }
    }
    @Override
    public long importFrom(InputStream content, ContentClaim claim) throws IOException {
        try (OutputStream outStream = write(claim)) {
            return StreamUtils.copy(content, outStream);
        }
    }
    @Override
    public long importFrom(java.nio.file.Path content, ContentClaim claim) throws IOException {
        try (InputStream inStream = Files.newInputStream(content, StandardOpenOption.READ)) {
            return importFrom(inStream, claim);
        }
    }

    @Override
    public long merge(Collection<ContentClaim> claims, ContentClaim destination, byte[] header, byte[] footer,
            byte[] demarcator) throws IOException {
        if (claims.contains(destination)) {
            throw new IllegalArgumentException("destination cannot be within claims");
        }

        try (final ByteCountingOutputStream out = new ByteCountingOutputStream(write(destination))) {
            if (header != null) {
                out.write(header);
            }

            int i = 0;
            for (final ContentClaim claim : claims) {
                try (final InputStream in = read(claim)) {
                    StreamUtils.copy(in, out);
                }

                if (++i < claims.size() && demarcator != null) {
                    out.write(demarcator);
                }
            }

            if (footer != null) {
                out.write(footer);
            }

            return out.getBytesWritten();
        }
    }

    @Override
    public boolean isActiveResourceClaimsSupported() {
        return true;
    }

    @Override
    public void shutdown() {
        if (workThreads != null) {
            workThreads.shutdown();
        }

        // Close any of the writable claim streams that are currently open.
        // Other threads may be writing to these streams, and that's okay.
        // If that happens, we will simply close the stream, resulting in an
        // IOException that will roll back the session. Since this is called
        // only on shutdown of the application, we don't have to worry about
        // partially written files - on restart, we will simply start writing
        // to new files and leave those trailing bytes alone.
        for (final OutputStream out : writableClaimStreams.values()) {
            try {
                out.close();
            } catch (IOException ex) { }
        }
    }

    @Override
    public void claimClosed(ClaimOutputStream claimStream) throws IOException {

        StandardContentClaim claim = claimStream.getClaim();

        if (repoConfig.isAlwaysSync()) {
            FSDataOutputStream fsOutStream = (FSDataOutputStream)claimStream.getOutStream().getWrappedStream();
            fsOutStream.hsync();
        } else if (claimStream.canRecycle()) {
            // flush from fs data output stream to the disk. This happens naturally
            // with FileOutputStream, but not with hadoop's FSDataOutputStream for obvious reasons
            claimStream.getOutStream().flush();
        }

        if (claim.getLength() < 0) {
            // If claim was not written to, set length to 0
            claim.setLength(0L);
        }

        // if we've not yet hit the threshold for appending to a resource claim, add the claim
        // to the writableClaimQueue so that the Resource Claim can be used again when create()
        // is called. In this case, we don't have to actually close the file stream. Instead, we
        // can just add it onto the queue and continue to use it for the next content claim.
        long resourceClaimLength = claim.getOffset() + claim.getLength();
        if (claimStream.canRecycle() && resourceClaimLength < repoConfig.getMaxAppendableClaimLength()) {
            ClaimAndLength claimAndLength = new ClaimAndLength(claim.getResourceClaim(), resourceClaimLength);

            // We are checking that writableClaimStreams contains the resource claim as a key, as a sanity check.
            // It should always be there. However, we have encountered a bug before where we archived content before
            // we should have. As a result, the Resource Claim and the associated OutputStream were removed from the
            // writableClaimStreams map, and this caused a NullPointerException. Worse, the call here to
            // writableClaimQueue.offer() means that the ResourceClaim was then reused, which resulted in an endless
            // loop of NullPointerException's being thrown. As a result, we simply ensure that the Resource Claim does
            // in fact have an OutputStream associated with it before adding it back to the writableClaimQueue.
            boolean enqueued = writableClaimStreams.get(claim.getResourceClaim()) != null && writableClaims.offer(claimAndLength);

            if (enqueued) {
                LOG.debug("Claim length less than max; Adding {} back to Writable Claim Queue", this);
            } else {
                writableClaimStreams.remove(claim.getResourceClaim());
                claimManager.freeze(claim.getResourceClaim());

                claimStream.getOutStream().close();

                LOG.debug("Claim length less than max; Closing {} because could not add back to queue", this);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Stack trace: ", new RuntimeException("Stack Trace for closing " + this));
                }
            }
        } else {
            // we've reached the limit for this claim. Don't add it back to our queue.
            // Instead, just remove it and move on.

            // Mark the claim as no longer being able to be written to
            claimManager.freeze(claim.getResourceClaim());

            // ensure that the claim is no longer on the queue
            writableClaims.remove(new ClaimAndLength(claim.getResourceClaim(), resourceClaimLength));

            claimStream.getOutStream().close();
            LOG.debug("Claim lenth >= max; Closing {}", this);
            if (LOG.isTraceEnabled()) {
                LOG.trace("Stack trace: ", new RuntimeException("Stack Trace for closing " + this));
            }
        }
    }

    @Override
    public void close() {
        shutdown();
    }
}
