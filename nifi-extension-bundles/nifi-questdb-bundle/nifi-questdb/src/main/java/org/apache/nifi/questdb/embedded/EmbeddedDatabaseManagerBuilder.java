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

import org.apache.nifi.questdb.DatabaseManager;
import org.apache.nifi.questdb.rollover.RolloverStrategy;

import java.io.File;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class EmbeddedDatabaseManagerBuilder {
    private SimpleEmbeddedDatabaseManagerContext context;

    private EmbeddedDatabaseManagerBuilder(final String persistPath) {
        this.context = new SimpleEmbeddedDatabaseManagerContext();
        context.setPersistLocation(persistPath);
    }

    public EmbeddedDatabaseManagerBuilder lockAttemptTime(final int lockAttemptTime, final TimeUnit lockAttemptTimeUnit) {
        context.setLockAttemptDuration(Duration.of(lockAttemptTime, lockAttemptTimeUnit.toChronoUnit()));
        return this;
    }

    public EmbeddedDatabaseManagerBuilder rolloverFrequency(final int rolloverFrequency, final TimeUnit rolloverFrequencyTimeUnit) {
        context.setRolloverFrequencyDuration(Duration.of(rolloverFrequency, rolloverFrequencyTimeUnit.toChronoUnit()));
        return this;
    }

    public EmbeddedDatabaseManagerBuilder numberOfAttemptedRetries(final int numberOfAttemptedRetries) {
        context.setNumberOfAttemptedRetries(numberOfAttemptedRetries);
        return this;
    }

    public EmbeddedDatabaseManagerBuilder backupLocation(final String backupLocation) {
        context.setBackupLocation(backupLocation);
        return this;
    }

    public EmbeddedDatabaseManagerBuilder addTable(final String name, final String definition) {
        return addTable(name, definition, RolloverStrategy.keep());
    }

    public EmbeddedDatabaseManagerBuilder addTable(final String name, final String definition, final RolloverStrategy rolloverStrategy) {
        context.addTableDefinition(new ManagedTableDefinition(name, definition, rolloverStrategy));
        return this;
    }

    public DatabaseManager build() {
        Objects.requireNonNull(context.getLockAttemptTime(), "Lock attempt must be specified");

        if (context.getLockAttemptTime().toMillis() <= 0) {
            throw new IllegalArgumentException("Lock attempt time must be bigger than 0");
        }

        Objects.requireNonNull(context.getRolloverFrequency(), "Rollover frequency must be specified");

        if (context.getRolloverFrequency().toMillis() <= 0) {
            throw new IllegalArgumentException("Rollover frequency must be bigger than 0");
        }

        if (context.getNumberOfAttemptedRetries() < 1) {
            throw new IllegalArgumentException("Number of attempted retries must be at least 1");
        }

        if (context.getTableDefinitions().isEmpty()) {
            throw new IllegalArgumentException("There must be at least on table specified");
        }

        if (context.getBackupLocation() == null) {
            context.setBackupLocation(new File(context.getPersistLocationAsPath().toFile().getParentFile(), "questDbBackup").getAbsolutePath());
        }

        final DatabaseManager result = new EmbeddedDatabaseManager(context);
        result.init();
        return result;
    }

    public static EmbeddedDatabaseManagerBuilder builder(final String persistPath) {
        return new EmbeddedDatabaseManagerBuilder(persistPath);
    }
}
