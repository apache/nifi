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

import static org.apache.nifi.hdfs.repository.HdfsContentRepository.CORE_SITE_DEFAULT_PROPERTY;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerGroup implements Iterable<Container> {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsContentRepository.class);
    private final Map<String, Container> byName;
    private final List<Container> all;
    private final int numContainers;

    /**
     * Creates a new container group based on the specified existing containers.
     */
    public ContainerGroup(Collection<Container> containers) {
        Map<String, Container> byName = new HashMap<>();
        for (Container container : containers) {
            byName.put(container.getName(), container);
        }
        this.byName = byName;
        this.all = new ArrayList<>(new TreeMap<String, Container>(byName).values());
        this.numContainers = this.all.size();
    }

    /**
     * Creates a group of containers as they are defined in the nifi properties
     * whose ids match the ones specified. This also ensures each container is
     * properly configured and the default directory structure is present.
     */
    public ContainerGroup(NiFiProperties properties, RepositoryConfig repoConfig, Set<String> include,
            Set<String> exclude) {
        Configuration defaultHdfsConfig = null;
        if (properties.getProperty(HdfsContentRepository.CORE_SITE_DEFAULT_PROPERTY) != null) {
            defaultHdfsConfig = new Configuration();
            defaultHdfsConfig.addResource(new Path(
                    verifyExists(properties.getProperty(CORE_SITE_DEFAULT_PROPERTY), CORE_SITE_DEFAULT_PROPERTY)));
        }

        if (include != null) {
            include = new HashSet<>(include);
        }

        Map<String, Container> byName = new HashMap<>();
        for (Entry<String, java.nio.file.Path> entry : properties.getContentRepositoryPaths().entrySet()) {
            String name = entry.getKey();
            if (include != null && !include.contains(name)) {
                continue;
            } else if (exclude != null && exclude.contains(name)) {
                continue;
            }

            Configuration config = defaultHdfsConfig;
            String coreSitePath = properties.getProperty(CORE_SITE_DEFAULT_PROPERTY + "." + name);
            if (coreSitePath != null) {
                config = new Configuration();
                config.addResource(new Path(verifyExists(coreSitePath, CORE_SITE_DEFAULT_PROPERTY + "." + name)));
            } else if (defaultHdfsConfig == null) {
                throw new RuntimeException("No core.site.xml defined for content repository container with name: "
                        + name + " one must be defined with '" + CORE_SITE_DEFAULT_PROPERTY + "' or '"
                        + CORE_SITE_DEFAULT_PROPERTY + "." + name + "'");
            }

            FileSystem fs;
            try {
                fs = FileSystem.get(config);
            } catch (IOException ex) {
                throw new RuntimeException(
                        "Could not create FileSystem for content repository container with name: " + name, ex);
            }

            // we can't have relative paths with a scheme, so resolve the
            // relative path if we have one and the scheme is 'file:' (local)
            String pathStr = entry.getValue().toString();
            Path path;
            if (pathStr.startsWith("file:") && pathStr.length() > 5 && pathStr.charAt(5) != '/') {
                path = new Path("file:" + new File(pathStr.substring(5)).getAbsolutePath());
            } else {
                path = new Path(pathStr);
            }

            if (pathStr.startsWith("file:")) {
                config.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
            }

            try {
                if (!fs.exists(path)) {
                    if (!fs.mkdirs(path)) {
                        throw new RuntimeException("Could not create content repository directory: '" + path
                                + "' for repository container with name: " + name);
                    }
                }
            } catch (IOException ex) {
                throw new RuntimeException("Could not verify FileSystem for content repository directory exists: '"
                        + path + "' for repository container with name: " + name, ex);
            }

            FsStatus status;
            try {
                status = fs.getStatus(path);
            } catch (IOException ex) {
                throw new RuntimeException(
                        "Could not get file system status for content repository container with name: " + name, ex);
            }

            int replication = config.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
            long capacity = status.getCapacity() / replication;
            if (capacity == 0) {
                throw new RuntimeException("System returned total space of the partition for " + name
                        + " is zero byte. Nifi can not create a zero sized content repository");
            }
            long maxArchiveBytes = (long) (capacity * (1D - (repoConfig.getMaxArchiveRatio() - 0.02)));
            long fullThreshold = (long) (repoConfig.getFullPercentage() * capacity);
            boolean pauseOnFailure = repoConfig.getFailureTimeoutMs() > 0;

            Container container = new Container(name, path, config, maxArchiveBytes, fullThreshold, pauseOnFailure);

            createDirectoryStructure(repoConfig, name, fs, path);

            byName.put(name, container);
        }
        this.byName = byName;
        this.all = new ArrayList<>(byName.values());
        this.numContainers = this.all.size();

        if (numContainers == 0) {
            if (include == null && exclude == null) {
                throw new RuntimeException("No content repository containers/directories specified");
            } else if (include == null) {
                throw new RuntimeException("No content repository containers specified whose id isn't one of: "
                        + StringUtils.join(exclude, ", "));
            } else {
                throw new RuntimeException("No containers found with an id of: " + StringUtils.join(include, ", "));
            }
        } else if (include != null) {
            include.removeAll(byName.keySet());
            if (include.size() > 0) {
                throw new RuntimeException("The following container ids should have existed, but were not found: "
                        + StringUtils.join(include, ", "));
            }
        }
    }

    private void createDirectoryStructure(RepositoryConfig repoConfig, String name, FileSystem fs, Path path) {
        // this is too slow if we do this with a single thread
        LOG.info("Verifying content repository container directory structure for: " + name + " / " + path);
        int threads = Math.max(Math.min(20, (repoConfig.getSectionsPerContainer() / 20)), 1);
        ExecutorService executor = Executors.newFixedThreadPool(threads,
                new NamedThreadFactory("Container Directory Structure Creator: " + name));
        try {
            List<Future<Boolean>> futures = new ArrayList<>();
            int each = repoConfig.getSectionsPerContainer() / threads;
            int extra = repoConfig.getSectionsPerContainer() % threads;
            for (int i = 0; i < threads; i++) {
                int from = i * each;
                int to;
                if (i == threads - 1) {
                    to = from + each;
                } else {
                    to = from + each + extra;
                }
                futures.add(executor.submit(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws IOException {
                        for (int i = from; i < to; i++) {
                            Path archiveDir = new Path(new Path(path, "" + i), HdfsContentRepository.ARCHIVE_DIR_NAME);
                            if (!fs.exists(archiveDir)) {
                                if (!fs.mkdirs(archiveDir)) {
                                    throw new RuntimeException("Could not create section archive direcotry: "
                                            + archiveDir + " for content repository container with name: " + name);
                                }
                            }
                        }
                        return true;
                    }
                }));
            }
            long endTime = System.currentTimeMillis() + (1000 * 60 * 5);
            for (Future<Boolean> future : futures) {
                future.get(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } catch (ExecutionException ex) {
            throw new RuntimeException("Faild to create directory structure for " + name, ex);
        } catch (TimeoutException e) {
            throw new RuntimeException("Failed to create directory strcuture in a reasonable time frame for: " + name);
        } finally {
            executor.shutdownNow();
        }
    }

    private static URI verifyExists(String path, String property) {
        File file = new File(path);
        if (!file.exists()) {
            throw new RuntimeException("Path does not exist: " + path + " for property: '" + property + "'");
        }
        return file.getAbsoluteFile().toURI();
    }

    public int getNumContainers() {
        return numContainers;
    }

    public Map<String, Container> getAll() {
        return byName;
    }

    public Container get(String name) {
        return byName.get(name);
    }

    public Container atModIndex(long index) {
        int next = (int) index % numContainers;
        return all.get(next);
    }

    public Container nextActiveAtModIndex(long index) {
        int next = (int) index % numContainers;
        Container container = all.get(next);
        if (container.isActive()) {
            return container;
        }

        // find the next closest active container
        for (int i = next + 1; i < numContainers; i++) {
            container = all.get(i);
            if (container.isActive()) {
                return container;
            }
        }
        for (int i = 0; i < next; i++) {
            container = all.get(i);
            if (container.isActive()) {
                return container;
            }
        }

        // no containers are available, caller should be checking for this
        return null;
    }

    @Override
    public Iterator<Container> iterator() {
        return all.iterator();
    }

    @Override
    public String toString() {
        return all.toString();
    }
}
