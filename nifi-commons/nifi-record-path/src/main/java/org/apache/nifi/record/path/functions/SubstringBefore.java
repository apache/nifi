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

import java.util.stream.Stream;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.StandardFieldValue;
import org.apache.nifi.record.path.paths.RecordPathSegment;
import org.apache.nifi.record.path.util.RecordPathUtils;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

public class SubstringBefore extends RecordPathSegment {

    private final RecordPathSegment recordPath;
    private final RecordPathSegment searchValuePath;

    public SubstringBefore(final RecordPathSegment recordPath, final RecordPathSegment searchValue, final boolean absolute) {
        super("substringBefore", null, absolute);

        this.recordPath = recordPath;
        this.searchValuePath = searchValue;
    }

    @Override
    public Stream<FieldValue> evaluate(final RecordPathEvaluationContext context) {
        final Stream<FieldValue> fieldValues = recordPath.evaluate(context);
        return fieldValues.filter(fv -> fv.getValue() != null)
            .map(fv -> {
                final String searchValue = RecordPathUtils.getFirstStringValue(searchValuePath, context);
                if (searchValue == null || searchValue.isEmpty()) {
                    return fv;
                }

                final String value = DataTypeUtils.toString(fv.getValue(), (String) null);
                final int index = value.indexOf(searchValue);
                if (index < 0) {
                    return fv;
                }

                return new StandardFieldValue(value.substring(0, index), fv.getField(), fv.getParent().orElse(null));
            });
    }

}
