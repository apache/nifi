/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.minifi.bootstrap.configuration;

import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.NOTIFIER_PROPERTY_PREFIX;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * FileChangeNotifier provides a simple FileSystem monitor for detecting changes for a specified file as generated from its corresponding {@link Path}.  Upon modifications to the associated file,
 * associated listeners receive notification of a change allowing configuration logic to be reanalyzed.  The backing implementation is associated with a {@link ScheduledExecutorService} that
 * ensures continuity of monitoring.
 */
public class FileChangeNotifier implements Runnable, ConfigurationChangeNotifier {

    private Path configFile;
    private WatchService watchService;
    private long pollingSeconds;

    private ScheduledExecutorService executorService;
    private final Set<ConfigurationChangeListener> configurationChangeListeners = new HashSet<>();

    protected static final String CONFIG_FILE_PATH_KEY = NOTIFIER_PROPERTY_PREFIX + ".file.config.path";
    protected static final String POLLING_PERIOD_INTERVAL_KEY = NOTIFIER_PROPERTY_PREFIX + ".file.polling.period.seconds";

    protected static final int DEFAULT_POLLING_PERIOD_INTERVAL = 15;
    protected static final TimeUnit DEFAULT_POLLING_PERIOD_UNIT = TimeUnit.SECONDS;

    @Override
    public Set<ConfigurationChangeListener> getChangeListeners() {
        return Collections.unmodifiableSet(configurationChangeListeners);
    }

    @Override
    public void notifyListeners() {
        final File fileToRead = configFile.toFile();
        for (final ConfigurationChangeListener listener : getChangeListeners()) {
            try (final FileInputStream fis = new FileInputStream(fileToRead);) {
                listener.handleChange(fis);
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to read the changed file " + configFile, ex);
            }
        }
    }

    @Override
    public boolean registerListener(ConfigurationChangeListener listener) {
        return this.configurationChangeListeners.add(listener);
    }

    protected boolean targetChanged() {
        boolean targetChanged = false;

        final WatchKey watchKey = this.watchService.poll();

        if (watchKey == null) {
            return targetChanged;
        }

        for (WatchEvent<?> watchEvt : watchKey.pollEvents()) {
            final WatchEvent.Kind<?> evtKind = watchEvt.kind();

            final WatchEvent<Path> pathEvent = (WatchEvent<Path>) watchEvt;
            final Path changedFile = pathEvent.context();

            // determine target change by verifying if the changed file corresponds to the config file monitored for this path
            targetChanged = (evtKind == ENTRY_MODIFY && changedFile.equals(configFile.getName(configFile.getNameCount() - 1)));
        }

        // After completing inspection, reset for detection of subsequent change events
        boolean valid = watchKey.reset();
        if (!valid) {
            throw new IllegalStateException("Unable to reinitialize file system watcher.");
        }

        return targetChanged;
    }

    protected static WatchService initializeWatcher(Path filePath) {
        try {
            final WatchService fsWatcher = FileSystems.getDefault().newWatchService();
            final Path watchDirectory = filePath.getParent();
            watchDirectory.register(fsWatcher, ENTRY_MODIFY);

            return fsWatcher;
        } catch (IOException ioe) {
            throw new IllegalStateException("Unable to initialize a file system watcher for the path " + filePath, ioe);
        }
    }

    @Override
    public void run() {
        if (targetChanged()) {
            notifyListeners();
        }
    }

    @Override
    public void initialize(Properties properties) {
        final String rawPath = properties.getProperty(CONFIG_FILE_PATH_KEY);
        final String rawPollingDuration = properties.getProperty(POLLING_PERIOD_INTERVAL_KEY, Long.toString(DEFAULT_POLLING_PERIOD_INTERVAL));

        if (rawPath == null || rawPath.isEmpty()) {
            throw new IllegalArgumentException("Property, " + CONFIG_FILE_PATH_KEY + ", for the path of the config file must be specified.");
        }

        try {
            setConfigFile(Paths.get(rawPath));
            setPollingPeriod(Long.parseLong(rawPollingDuration), DEFAULT_POLLING_PERIOD_UNIT);
            setWatchService(initializeWatcher(configFile));
        } catch (Exception e) {
            throw new IllegalStateException("Could not successfully initialize file change notifier.", e);
        }
    }

    protected void setConfigFile(Path configFile) {
        this.configFile = configFile;
    }

    protected void setWatchService(WatchService watchService) {
        this.watchService = watchService;
    }

    protected void setPollingPeriod(long duration, TimeUnit unit) {
        if (duration < 0) {
            throw new IllegalArgumentException("Cannot specify a polling period with duration <=0");
        }
        this.pollingSeconds = TimeUnit.SECONDS.convert(duration, unit);
    }

    @Override
    public void start() {
        executorService = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable r) {
                final Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setName("File Change Notifier Thread");
                t.setDaemon(true);
                return t;
            }
        });
        this.executorService.scheduleWithFixedDelay(this, 0, pollingSeconds, DEFAULT_POLLING_PERIOD_UNIT);
    }

    @Override
    public void close() {
        if (this.executorService != null) {
            this.executorService.shutdownNow();
        }
    }
}

