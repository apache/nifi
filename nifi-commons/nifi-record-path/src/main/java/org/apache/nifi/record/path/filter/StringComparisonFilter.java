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

package org.apache.nifi.record.path.filter;

import java.util.Optional;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.paths.RecordPathSegment;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

public abstract class StringComparisonFilter extends FunctionFilter {

    private final RecordPathSegment searchValuePath;

    public StringComparisonFilter(RecordPathSegment recordPath, final RecordPathSegment searchValuePath) {
        super(recordPath);
        this.searchValuePath = searchValuePath;
    }

    @Override
    protected boolean test(final FieldValue fieldValue, final RecordPathEvaluationContext context) {
        final String fieldVal = DataTypeUtils.toString(fieldValue.getValue(), (String) null);
        if (fieldVal == null) {
            return false;
        }

        final Optional<FieldValue> firstValue = searchValuePath.evaluate(context).findFirst();
        if (!firstValue.isPresent()) {
            return false;
        }

        final String searchValue = DataTypeUtils.toString(firstValue.get().getValue(), (String) null);
        if (searchValue == null) {
            return false;
        }

        return isMatch(fieldVal, searchValue);
    }

    protected abstract boolean isMatch(String fieldValue, String comparison);
}
