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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.hadoop.util.FileStatusIterable;
import org.apache.nifi.processors.hadoop.util.FileStatusManager;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.OutputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.hadoop.ListHDFS.REL_SUCCESS;

public class RecordObjectWriter extends HdfsObjectWriter {

    private static final RecordSchema RECORD_SCHEMA;

    private static final String FILENAME = "filename";
    private static final String PATH = "path";
    private static final String IS_DIRECTORY = "directory";
    private static final String SIZE = "size";
    private static final String LAST_MODIFIED = "lastModified";
    private static final String PERMISSIONS = "permissions";
    private static final String OWNER = "owner";
    private static final String GROUP = "group";
    private static final String REPLICATION = "replication";
    private static final String IS_SYM_LINK = "symLink";
    private static final String IS_ENCRYPTED = "encrypted";
    private static final String IS_ERASURE_CODED = "erasureCoded";

    static {
        final List<RecordField> recordFields = new ArrayList<>();
        recordFields.add(new RecordField(FILENAME, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(PATH, RecordFieldType.STRING.getDataType(), false));
        recordFields.add(new RecordField(IS_DIRECTORY, RecordFieldType.BOOLEAN.getDataType(), false));
        recordFields.add(new RecordField(SIZE, RecordFieldType.LONG.getDataType(), false));
        recordFields.add(new RecordField(LAST_MODIFIED, RecordFieldType.TIMESTAMP.getDataType(), false));
        recordFields.add(new RecordField(PERMISSIONS, RecordFieldType.STRING.getDataType()));
        recordFields.add(new RecordField(OWNER, RecordFieldType.STRING.getDataType()));
        recordFields.add(new RecordField(GROUP, RecordFieldType.STRING.getDataType()));
        recordFields.add(new RecordField(REPLICATION, RecordFieldType.INT.getDataType()));
        recordFields.add(new RecordField(IS_SYM_LINK, RecordFieldType.BOOLEAN.getDataType()));
        recordFields.add(new RecordField(IS_ENCRYPTED, RecordFieldType.BOOLEAN.getDataType()));
        recordFields.add(new RecordField(IS_ERASURE_CODED, RecordFieldType.BOOLEAN.getDataType()));
        RECORD_SCHEMA = new SimpleRecordSchema(recordFields);
    }


    private final RecordSetWriterFactory writerFactory;
    private final ComponentLog logger;

    public RecordObjectWriter(ProcessSession session, FileStatusIterable fileStatuses, long minimumAge, long maximumAge, PathFilter pathFilter,
                              FileStatusManager fileStatusManager, long latestModificationTime, List<String> latestModifiedStatuses,
                              RecordSetWriterFactory writerFactory, ComponentLog logger) {
        super(session, fileStatuses, minimumAge, maximumAge, pathFilter, fileStatusManager, latestModificationTime, latestModifiedStatuses);
        this.writerFactory = writerFactory;
        this.logger = logger;
    }

    @Override
    public void write() {
        FlowFile flowFile = session.create();

        final OutputStream out = session.write(flowFile);
        try (RecordSetWriter recordWriter = writerFactory.createWriter(logger, RECORD_SCHEMA, out, flowFile)) {
            recordWriter.beginRecordSet();

            for (FileStatus status : fileStatuses) {
                if (determineListable(status, minimumAge, maximumAge, pathFilter, latestModificationTime, latestModifiedStatuses)) {
                    recordWriter.write(createRecordForListing(status));
                    fileStatusManager.update(status);
                }
            }

            WriteResult writeResult = recordWriter.finishRecordSet();
            fileCount = writeResult.getRecordCount();

            if (fileCount == 0) {
                session.remove(flowFile);
            } else {
                final Map<String, String> attributes = new HashMap<>(writeResult.getAttributes());
                attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                flowFile = session.putAllAttributes(flowFile, attributes);

                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (Exception e) {
            throw new ProcessException("An error occurred while writing results", e);
        }
    }

    private Record createRecordForListing(final FileStatus fileStatus) {
        final Map<String, Object> values = new HashMap<>();
        values.put(FILENAME, fileStatus.getPath().getName());
        values.put(PATH, getAbsolutePath(fileStatus.getPath().getParent()));
        values.put(OWNER, fileStatus.getOwner());
        values.put(GROUP, fileStatus.getGroup());
        values.put(LAST_MODIFIED, new Timestamp(fileStatus.getModificationTime()));
        values.put(SIZE, fileStatus.getLen());
        values.put(REPLICATION, fileStatus.getReplication());

        final FsPermission permission = fileStatus.getPermission();
        final String perms = getPermissionsString(permission);
        values.put(PERMISSIONS, perms);

        values.put(IS_DIRECTORY, fileStatus.isDirectory());
        values.put(IS_SYM_LINK, fileStatus.isSymlink());
        values.put(IS_ENCRYPTED, fileStatus.isEncrypted());
        values.put(IS_ERASURE_CODED, fileStatus.isErasureCoded());

        return new MapRecord(RECORD_SCHEMA, values);
    }

}

