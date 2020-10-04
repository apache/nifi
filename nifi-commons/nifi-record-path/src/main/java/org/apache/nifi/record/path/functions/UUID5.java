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

package org.apache.nifi.record.path.functions;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.StandardFieldValue;
import org.apache.nifi.record.path.paths.RecordPathSegment;

import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.nifi.uuid5.Uuid5Util.fromString;

// This is based on an unreleased implementation in Apache Commons Id. See http://svn.apache.org/repos/asf/commons/sandbox/id/trunk/src/java/org/apache/commons/id/uuid/UUID.java
public class UUID5 extends RecordPathSegment {
    private static final Object EMPTY_UUID = fromString("", null);
    private final RecordPathSegment recordPath;
    private final RecordPathSegment nameSegment;

    public UUID5(final RecordPathSegment recordPath, final RecordPathSegment nameSegment, final boolean absolute) {
        super("uuid5", null, absolute);
        this.recordPath = recordPath;
        this.nameSegment = nameSegment;
    }

    @Override
    public Stream<FieldValue> evaluate(RecordPathEvaluationContext context) {
        return recordPath.evaluate(context).map(fv -> {
            Optional<String> nameOpt = getName(context);

            if (fv.getValue() == null) {
                return new StandardFieldValue(EMPTY_UUID, fv.getField(), fv.getParent().orElse(null));
            }

            String fieldValue = fv.getValue().toString();

            String v5Uuid = fromString(fieldValue, nameOpt.orElse(null));

            return new StandardFieldValue(v5Uuid, fv.getField(), fv.getParent().orElse(null));
        });
    }

    private Optional<String> getName(final RecordPathEvaluationContext context) {
        if (nameSegment == null) {
            return Optional.empty();
        }

        Optional<FieldValue> opt = nameSegment.evaluate(context).findFirst();
        if (!opt.isPresent()) {
            return Optional.empty();
        }

        FieldValue value = opt.get();
        Object rawValue = value.getValue();

        if (rawValue instanceof String) {
            return Optional.of((String)rawValue);
        } else {
            return Optional.empty();
        }
    }
}
