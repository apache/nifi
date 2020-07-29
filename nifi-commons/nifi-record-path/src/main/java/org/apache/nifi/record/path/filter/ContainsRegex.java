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
import java.util.regex.Pattern;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.paths.LiteralValuePath;
import org.apache.nifi.record.path.paths.RecordPathSegment;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

public class ContainsRegex extends FunctionFilter {

    private final RecordPathSegment regexPath;

    private final Pattern compiledPattern;

    public ContainsRegex(RecordPathSegment recordPath, final RecordPathSegment regexPath) {
        super(recordPath);
        this.regexPath = regexPath;

        if (regexPath instanceof LiteralValuePath) {
            final FieldValue fieldValue = ((LiteralValuePath) regexPath).evaluate((RecordPathEvaluationContext) null).findFirst().get();
            final Object value = fieldValue.getValue();
            final String regex = DataTypeUtils.toString(value, (String) null);
            compiledPattern = Pattern.compile(regex);
        } else {
            compiledPattern = null;
        }
    }

    @Override
    protected boolean test(final FieldValue fieldValue, final RecordPathEvaluationContext context) {
        final Pattern pattern;
        if (compiledPattern == null) {
            final Optional<FieldValue> fieldValueOption = regexPath.evaluate(context).findFirst();
            if (!fieldValueOption.isPresent()) {
                return false;
            }

            final Object value = fieldValueOption.get().getValue();
            if (value == null) {
                return false;
            }

            final String regex = DataTypeUtils.toString(value, (String) null);
            pattern = Pattern.compile(regex);
        } else {
            pattern = compiledPattern;
        }

        final String searchString = DataTypeUtils.toString(fieldValue.getValue(), (String) null);
        if (searchString == null) {
            return false;
        }

        return pattern.matcher(searchString).find();
    }

}
