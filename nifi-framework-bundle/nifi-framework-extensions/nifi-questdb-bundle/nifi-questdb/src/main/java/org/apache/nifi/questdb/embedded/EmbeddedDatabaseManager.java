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
package org.apache.nifi.questdb.embedded;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableRecordMetadata;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.nifi.questdb.Client;
import org.apache.nifi.questdb.DatabaseException;
import org.apache.nifi.questdb.DatabaseManager;
import org.apache.nifi.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

final class EmbeddedDatabaseManager implements DatabaseManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedDatabaseManager.class);

    private final String id = UUID.randomUUID().toString();
    private final AtomicReference<EmbeddedDatabaseManagerStatus> state = new AtomicReference<>(EmbeddedDatabaseManagerStatus.UNINITIALIZED);
    private final ReadWriteLock databaseStructureLock = new ReentrantReadWriteLock();
    private final EmbeddedDatabaseManagerContext context;
    private final AtomicReference<CairoEngine> engine = new AtomicReference<>();
    private final List<ScheduledFuture<?>> scheduledFutures = new ArrayList<>();
    private final ScheduledExecutorService scheduledExecutorService = Executors
            .newScheduledThreadPool(2, new BasicThreadFactory.Builder().namingPattern("EmbeddedQuestDbManagerWorker-" + id + "-%d").build());

    EmbeddedDatabaseManager(final EmbeddedDatabaseManagerContext context) {
        this.context = context;
    }

    @Override
    public void init() {
        if (state.get() != EmbeddedDatabaseManagerStatus.UNINITIALIZED) {
            throw new IllegalStateException("Manager is already initialized");
        }

        ensureDatabaseIsReady();
        startRollover();
    }

    private void ensureDatabaseIsReady() {
        boolean successful = false;

        try {
            databaseStructureLock.writeLock().lock();
            state.set(EmbeddedDatabaseManagerStatus.REPAIRING);

            try {
                ensurePersistLocationIsAccessible();
                ensureConnectionEstablished();
                ensureTablesAreInPlaceAndHealthy();
                successful = true;
            } catch (final CorruptedDatabaseException e) {
                boolean couldMoveOldToBackup = false;

                try {
                    LOGGER.error("Database is corrupted. Recreation is triggered. Manager tries to move corrupted database files to the backup location: {}", context.getBackupLocation(), e);
                    final File backupFolder = new File(context.getBackupLocationAsPath().toFile(), "backup_" + System.currentTimeMillis());
                    FileUtils.ensureDirectoryExistAndCanAccess(context.getBackupLocationAsPath().toFile());
                    Files.move(context.getPersistLocationAsPath(), backupFolder.toPath());
                    couldMoveOldToBackup = true;
                } catch (IOException ex) {
                    LOGGER.error("Could not create backup", ex);
                }

                if (!couldMoveOldToBackup) {
                    try {
                        FileUtils.deleteFile(context.getPersistLocationAsPath().toFile(), true);
                        couldMoveOldToBackup = true;
                    } catch (IOException ex) {
                        LOGGER.error("Could not clean up corrupted database", ex);
                    }
                }

                if (couldMoveOldToBackup) {
                    try {
                        ensurePersistLocationIsAccessible();
                        ensureConnectionEstablished();
                        ensureTablesAreInPlaceAndHealthy();
                        successful = true;
                    } catch (CorruptedDatabaseException ex) {
                        LOGGER.error("Could not create backup", ex);
                    }
                }
            }
        } finally {
            state.set(successful ? EmbeddedDatabaseManagerStatus.HEALTHY : EmbeddedDatabaseManagerStatus.CORRUPTED);

            if (!successful) {
                engine.set(null);
            }

            databaseStructureLock.writeLock().unlock();
        }

    }

    private void ensurePersistLocationIsAccessible() throws CorruptedDatabaseException {
        try {
            FileUtils.ensureDirectoryExistAndCanAccess(context.getPersistLocationAsPath().toFile());
        } catch (final Exception e) {
            throw new CorruptedDatabaseException(String.format("Database directory creation failed [%s]", context.getPersistLocationAsPath()), e);
        }
    }

    private void ensureConnectionEstablished() throws CorruptedDatabaseException {
        if (engine.get() != null) {
            engine.getAndSet(null).close();
        }

        final String absolutePath = context.getPersistLocationAsPath().toFile().getAbsolutePath();
        final CairoConfiguration configuration = new DefaultCairoConfiguration(absolutePath);

        try {
            final CairoEngine engine = new CairoEngine(configuration);
            LOGGER.info("Database connection successful [{}]", absolutePath);
            this.engine.set(engine);
        } catch (final Exception e) {
            throw new CorruptedDatabaseException(String.format("Database connection failed [%s]", absolutePath), e);
        }
    }

    private void ensureTablesAreInPlaceAndHealthy() throws CorruptedDatabaseException {
        final Map<String, File> databaseFiles = Arrays.stream(context.getPersistLocationAsPath().toFile().listFiles())
                .collect(Collectors.toMap(f -> f.getAbsolutePath().substring(context.getPersistLocationAsPath().toFile().getAbsolutePath().length() + 1), f -> f));
        final Client client = getUnmanagedClient();

        try {
            for (final ManagedTableDefinition tableDefinition : context.getTableDefinitions()) {
                if (!databaseFiles.containsKey(tableDefinition.getName())) {
                    try {
                        LOGGER.debug("Creating table {}", tableDefinition.getName());
                        client.execute(tableDefinition.getDefinition());
                        LOGGER.debug("Table {} is created", tableDefinition.getName());
                    } catch (DatabaseException e) {
                        throw new CorruptedDatabaseException(String.format("Creating table [%s] has failed", tableDefinition.getName()), e);
                    }
                } else if (!databaseFiles.get(tableDefinition.getName()).isDirectory()) {
                    throw new CorruptedDatabaseException(String.format("Table %s cannot be created because there is already a file exists with the given name", tableDefinition.getName()));
                }
            }

            // Checking if tables are healthy.
            for (final ManagedTableDefinition tableDefinition : context.getTableDefinitions()) {
                try {
                    final TableToken tableToken = this.engine.get().getTableTokenIfExists(tableDefinition.getName());
                    if (tableToken.isWal()) {
                        final TableRecordMetadata metadata = this.engine.get().getSequencerMetadata(tableToken);
                        metadata.close();
                    }

                    client.execute(String.format("SELECT * FROM %S LIMIT 1", tableDefinition.getName()));
                } catch (final Exception e) {
                    throw new CorruptedDatabaseException(e);
                }
            }
        } finally {
            try {
                client.disconnect();
            } catch (DatabaseException e) {
                throw new CorruptedDatabaseException(e);
            }
        }
    }

    private void startRollover() {
        final RolloverWorker rolloverWorker = new RolloverWorker(acquireClient(), context.getTableDefinitions());
        final ScheduledFuture<?> rolloverFuture = scheduledExecutorService.scheduleWithFixedDelay(
                rolloverWorker, context.getRolloverFrequency().toMillis(), context.getRolloverFrequency().toMillis(), TimeUnit.MILLISECONDS);
        scheduledFutures.add(rolloverFuture);
        LOGGER.debug("Rollover started");
    }

    private void stopRollover() {
        LOGGER.debug("Rollover shutdown started");

        int cancelCompleted = 0;
        int cancelFailed = 0;
        for (final ScheduledFuture<?> scheduledFuture : scheduledFutures) {
            final boolean cancelled = scheduledFuture.cancel(true);
            if (cancelled) {
                cancelCompleted++;
            } else {
                cancelFailed++;
            }
        }

        LOGGER.debug("Rollover shutdown task cancellation status: completed [{}] failed [{}]", cancelCompleted, cancelFailed);
        final List<Runnable> tasks = scheduledExecutorService.shutdownNow();
        LOGGER.debug("Rollover Scheduled Task Service shutdown remaining tasks [{}]", tasks.size());
    }

    private Client getUnmanagedClient() {
        return new EmbeddedClient(() -> engine.get());
    }

    @Override
    public Client acquireClient() {
        checkIfManagerIsInitialised();
        final Client fallback = new NoOpClient();

        if (state.get() == EmbeddedDatabaseManagerStatus.CORRUPTED) {
            LOGGER.error("The database is corrupted: Status History will not be stored");
            return fallback;
        }

        final LockedClient lockedClient = new LockedClient(
                databaseStructureLock.readLock(),
                context.getLockAttemptTime(),
                new ConditionAwareClient(() -> state.get() == EmbeddedDatabaseManagerStatus.HEALTHY, getUnmanagedClient())
        );

        return RetryingClient.getInstance(context.getNumberOfAttemptedRetries(), this::errorAction, lockedClient, fallback);
    }

    private void checkIfManagerIsInitialised() {
        if (state.get() == EmbeddedDatabaseManagerStatus.UNINITIALIZED) {
            throw new IllegalStateException("The state of the database manager is not initialized");
        }
    }

    private void errorAction(final int attemptNumber, final Throwable throwable) {
        if (shouldRestoreDatabase(attemptNumber, throwable)) {
            LOGGER.error("Database manager tries to restore database after the first failed attempt if necessary");
            ensureDatabaseIsReady();
        } else {
            LOGGER.warn("Error happened at attempt: {}", attemptNumber, throwable);
        }
    }

    private boolean shouldRestoreDatabase(final int attemptNumber, final Throwable throwable) {
        if (state.get() == EmbeddedDatabaseManagerStatus.CORRUPTED
                || state.get() == EmbeddedDatabaseManagerStatus.CLOSED
        ) {
            return false;
        }

        if (throwable instanceof ConditionFailedException
            || throwable instanceof LockUnsuccessfulException
            || throwable instanceof ClientDisconnectedException
        ) {
            return false;
        }

        return attemptNumber == 1;
    }

    @Override
    public void close() {
        databaseStructureLock.writeLock().lock();

        checkIfManagerIsInitialised();
        stopRollover();
        state.set(EmbeddedDatabaseManagerStatus.CLOSED);
        final CairoEngine engine = this.engine.get();

        if (engine != null) {
            engine.close();
        }

        databaseStructureLock.writeLock().unlock();
    }
}
