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

package org.apache.nifi.record.path.paths;

import java.util.Objects;
import java.util.stream.Stream;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.StandardRecordPathEvaluationContext;
import org.apache.nifi.serialization.record.Record;

public abstract class RecordPathSegment implements RecordPath {
    private final String path;
    private final RecordPathSegment parentPath;
    private final boolean absolute;

    public RecordPathSegment(final String path, final RecordPathSegment parentPath, final boolean absolute) {
        this.path = path;
        this.parentPath = parentPath;
        this.absolute = absolute;
    }

    @Override
    public String getPath() {
        return path;
    }

    RecordPathSegment getParentPath() {
        return parentPath;
    }

    @Override
    public String toString() {
        return getPath();
    }

    @Override
    public boolean isAbsolute() {
        return absolute;
    }

    @Override
    public int hashCode() {
        return Objects.hash(path);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof RecordPath)) {
            return false;
        }

        final RecordPath other = (RecordPath) obj;
        return getPath().equals(other.getPath());
    }

    @Override
    public final RecordPathResult evaluate(final Record record) {
        final RecordPathEvaluationContext context = new StandardRecordPathEvaluationContext(record);
        final Stream<FieldValue> selectedFields = evaluate(context);

        return new RecordPathResult() {
            @Override
            public String getPath() {
                return RecordPathSegment.this.getPath();
            }

            @Override
            public Stream<FieldValue> getSelectedFields() {
                return selectedFields;
            }
        };
    }

    @Override
    public final RecordPathResult evaluate(final Record record, final FieldValue contextNode) {
        final RecordPathEvaluationContext context = new StandardRecordPathEvaluationContext(record);
        context.setContextNode(contextNode);
        final Stream<FieldValue> selectedFields = evaluate(context);

        return new RecordPathResult() {
            @Override
            public String getPath() {
                return RecordPathSegment.this.getPath();
            }

            @Override
            public Stream<FieldValue> getSelectedFields() {
                return selectedFields;
            }
        };
    }

    public abstract Stream<FieldValue> evaluate(RecordPathEvaluationContext context);
}
