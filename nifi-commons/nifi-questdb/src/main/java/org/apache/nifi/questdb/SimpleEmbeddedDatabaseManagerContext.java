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
package org.apache.nifi.questdb;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

final class SimpleEmbeddedDatabaseManagerContext implements EmbeddedDatabaseManagerContext {
    final private Set<ManagedTableDefinition> tableDefinitions = new HashSet<>();

    private String persistLocation;
    private String backupLocation;
    private int numberOfAttemptedRetries;
    private int lockAttemptTime;
    private TimeUnit lockAttemptTimeUnit;
    private int rolloverFrequency;
    private TimeUnit rolloverFrequencyTimeUnit;

    @Override
    public String getPersistLocation() {
        return persistLocation;
    }

    @Override
    public Path getPersistLocationAsPath() {
        return Paths.get(getPersistLocation());
    }

    @Override
    public File getPersistLocationAsFile() {
        return getPersistLocationAsPath().toFile();
    }

    @Override
    public String getBackupLocation() {
        return backupLocation;
    }

    @Override
    public Path getBackupLocationAsPath() {
        return Paths.get(getBackupLocation());
    }

    @Override
    public File getBackupLocationAsFile() {
        return getBackupLocationAsPath().toFile();
    }

    @Override
    public int getNumberOfAttemptedRetries() {
        return numberOfAttemptedRetries;
    }

    @Override
    public int getLockAttemptTime() {
        return lockAttemptTime;
    }

    @Override
    public int getRolloverFrequency() {
        return rolloverFrequency;
    }

    @Override
    public TimeUnit getRolloverFrequencyTimeUnit() {
        return rolloverFrequencyTimeUnit;
    }

    @Override
    public TimeUnit getLockAttemptTimeUnit() {
        return lockAttemptTimeUnit;
    }

    @Override
    public Set<ManagedTableDefinition> getTableDefinitions() {
        return tableDefinitions;
    }

    void setPersistLocation(final String persistLocation) {
        this.persistLocation = persistLocation;
    }

    public void setBackupLocation(final String backupLocation) {
        this.backupLocation = backupLocation;
    }

    void setNumberOfAttemptedRetries(final int numberOfAttemptedRetries) {
        this.numberOfAttemptedRetries = numberOfAttemptedRetries;
    }

    void setLockAttemptTime(final int lockAttemptTime) {
        this.lockAttemptTime = lockAttemptTime;
    }

    void setLockAttemptTimeUnit(final TimeUnit lockAttemptTimeUnit) {
        this.lockAttemptTimeUnit = lockAttemptTimeUnit;
    }

    public void setRolloverFrequency(final int rolloverFrequency) {
        this.rolloverFrequency = rolloverFrequency;
    }

    public void setRolloverFrequencyTimeUnit(final TimeUnit rolloverFrequencyTimeUnit) {
        this.rolloverFrequencyTimeUnit = rolloverFrequencyTimeUnit;
    }

    void addTableDefinition(final ManagedTableDefinition tableDefinition) {
        tableDefinitions.add(tableDefinition);
    }

    @Override
    public String toString() {
        return "SimpleEmbeddedQuestDbManagerContext{" +
                "tableDefinitions=" + tableDefinitions +
                ", path='" + persistLocation + '\'' +
                ", backupLocation='" + backupLocation + '\'' +
                ", numberOfAttemptedRetries=" + numberOfAttemptedRetries +
                ", lockAttemptTime=" + lockAttemptTime +
                ", lockAttemptTimeUnit=" + lockAttemptTimeUnit +
                ", rolloverFrequency=" + rolloverFrequency +
                ", rolloverFrequencyTimeUnit=" + rolloverFrequencyTimeUnit +
                '}';
    }
}
