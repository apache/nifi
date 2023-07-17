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
package org.apache.nifi.processors.hadoop.util.writer;

import org.apache.hadoop.fs.PathFilter;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.hadoop.util.FileStatusIterable;
import org.apache.nifi.processors.hadoop.util.FileStatusManager;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.util.List;

public class HadoopWriterContext {

    private final ProcessSession session;
    private final Relationship successRelationship;
    private final FileStatusIterable fileStatusIterable;
    private final FileStatusManager fileStatusManager;
    private final PathFilter pathFilter;
    private final long minimumAge;
    private final long maximumAge;
    private final long previousLatestTimestamp;
    private final List<String> previousLatestFiles;
    private final RecordSetWriterFactory writerFactory;
    private final String hdfsPrefix;
    private final ComponentLog logger;


    public HadoopWriterContext(final ProcessSession session,
                               final Relationship successRelationship,
                               final FileStatusIterable fileStatusIterable,
                               final FileStatusManager fileStatusManager,
                               final PathFilter pathFilter,
                               final long minimumAge,
                               final long maximumAge,
                               final long previousLatestTimestamp,
                               final List<String> previousLatestFiles,
                               final RecordSetWriterFactory writerFactory,
                               final String hdfsPrefix,
                               final ComponentLog logger) {
        this.session = session;
        this.successRelationship = successRelationship;
        this.fileStatusIterable = fileStatusIterable;
        this.fileStatusManager = fileStatusManager;
        this.pathFilter = pathFilter;
        this.minimumAge = minimumAge;
        this.maximumAge = maximumAge;
        this.previousLatestTimestamp = previousLatestTimestamp;
        this.previousLatestFiles = previousLatestFiles;
        this.writerFactory = writerFactory;
        this.hdfsPrefix = hdfsPrefix;
        this.logger = logger;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private ProcessSession session;
        private FileStatusIterable fileStatusIterable;
        private long minimumAge;
        private long maximumAge;
        private PathFilter pathFilter;
        private FileStatusManager fileStatusManager;
        private long previousLatestTimestamp;
        private List<String> previousLatestFiles;
        private Relationship successRelationship;
        private RecordSetWriterFactory writerFactory;
        private ComponentLog logger;
        private String hdfsPrefix;

        public Builder session(final ProcessSession session) {
            this.session = session;
            return this;
        }

        public Builder fileStatusIterable(final FileStatusIterable fileStatusIterable) {
            this.fileStatusIterable = fileStatusIterable;
            return this;
        }

        public Builder minimumAge(final long minimumAge) {
            this.minimumAge = minimumAge;
            return this;
        }

        public Builder maximumAge(final long maximumAge) {
            this.maximumAge = maximumAge;
            return this;
        }

        public Builder pathFilter(final PathFilter pathFilter) {
            this.pathFilter = pathFilter;
            return this;
        }

        public Builder fileStatusManager(final FileStatusManager fileStatusManager) {
            this.fileStatusManager = fileStatusManager;
            return this;
        }

        public Builder previousLatestTimestamp(final long previousLatestTimestamp) {
            this.previousLatestTimestamp = previousLatestTimestamp;
            return this;
        }

        public Builder previousLatestFiles(final List<String> previousLatestFiles) {
            this.previousLatestFiles = previousLatestFiles;
            return this;
        }

        public Builder successRelationship(final Relationship successRelationship) {
            this.successRelationship = successRelationship;
            return this;
        }

        public Builder writerFactory(final RecordSetWriterFactory writerFactory) {
            this.writerFactory = writerFactory;
            return this;
        }

        public Builder logger(final ComponentLog logger) {
            this.logger = logger;
            return this;
        }

        public Builder hdfsPrefix(final String hdfsPrefix) {
            this.hdfsPrefix = hdfsPrefix;
            return this;
        }

        public HadoopWriterContext build() {
            return new HadoopWriterContext(session, successRelationship, fileStatusIterable, fileStatusManager, pathFilter, minimumAge, maximumAge,
                    previousLatestTimestamp, previousLatestFiles, writerFactory, hdfsPrefix, logger);
        }
    }

    public ProcessSession getSession() {
        return session;
    }

    public Relationship getSuccessRelationship() {
        return successRelationship;
    }

    public FileStatusIterable getFileStatusIterable() {
        return fileStatusIterable;
    }

    public FileStatusManager getFileStatusManager() {
        return fileStatusManager;
    }

    public PathFilter getPathFilter() {
        return pathFilter;
    }

    public long getMinimumAge() {
        return minimumAge;
    }

    public long getMaximumAge() {
        return maximumAge;
    }

    public long getPreviousLatestTimestamp() {
        return previousLatestTimestamp;
    }

    public List<String> getPreviousLatestFiles() {
        return previousLatestFiles;
    }

    public RecordSetWriterFactory getWriterFactory() {
        return writerFactory;
    }

    public String getHdfsPrefix() {
        return hdfsPrefix;
    }

    public ComponentLog getLogger() {
        return logger;
    }
}
