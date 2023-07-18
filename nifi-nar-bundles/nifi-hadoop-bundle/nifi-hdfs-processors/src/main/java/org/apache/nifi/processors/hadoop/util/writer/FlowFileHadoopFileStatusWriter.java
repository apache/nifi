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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.hadoop.util.FileStatusIterable;
import org.apache.nifi.processors.hadoop.util.FileStatusManager;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlowFileHadoopFileStatusWriter extends HadoopFileStatusWriter {
    public FlowFileHadoopFileStatusWriter(final ProcessSession session,
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
        super(session, successRelationship, fileStatusIterable, fileStatusManager, pathFilter, minimumAge, maximumAge, previousLatestTimestamp, previousLatestFiles, writerFactory, hdfsPrefix, logger);
    }

    @Override
    public void write() {
        for (FileStatus status : fileStatusIterable) {
            if (determineListable(status)) {

                final Map<String, String> attributes = createAttributes(status);
                FlowFile flowFile = session.create();
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.transfer(flowFile, successRelationship);

                fileStatusManager.update(status);
                fileCount++;
            }
        }
    }

    private Map<String, String> createAttributes(final FileStatus status) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), status.getPath().getName());
        attributes.put(CoreAttributes.PATH.key(), getAbsolutePath(status.getPath().getParent()));
        attributes.put(hdfsPrefix + ".owner", status.getOwner());
        attributes.put(hdfsPrefix + ".group", status.getGroup());
        attributes.put(hdfsPrefix + ".lastModified", String.valueOf(status.getModificationTime()));
        attributes.put(hdfsPrefix + ".length", String.valueOf(status.getLen()));
        attributes.put(hdfsPrefix + ".replication", String.valueOf(status.getReplication()));
        attributes.put(hdfsPrefix + ".permissions", getPermissionsString(status.getPermission()));
        return attributes;
    }
}
