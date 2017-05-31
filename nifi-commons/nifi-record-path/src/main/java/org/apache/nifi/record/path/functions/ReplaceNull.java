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

import java.util.Optional;
import java.util.stream.Stream;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.StandardFieldValue;
import org.apache.nifi.record.path.paths.RecordPathSegment;

public class ReplaceNull extends RecordPathSegment {

    private final RecordPathSegment recordPath;
    private final RecordPathSegment replacementValuePath;

    public ReplaceNull(final RecordPathSegment recordPath, final RecordPathSegment replacementValue, final boolean absolute) {
        super("replaceNull", null, absolute);

        this.recordPath = recordPath;
        this.replacementValuePath = replacementValue;
    }

    @Override
    public Stream<FieldValue> evaluate(final RecordPathEvaluationContext context) {
        final Stream<FieldValue> fieldValues = recordPath.evaluate(context);
        return fieldValues
            .map(fv -> {
                if (fv.getValue() != null) {
                    return fv;
                }

                final Optional<FieldValue> replacementOption = replacementValuePath.evaluate(context).findFirst();
                if (!replacementOption.isPresent()) {
                    return fv;
                }

                final FieldValue replacementFieldValue = replacementOption.get();
                final Object replacementValue = replacementFieldValue.getValue();
                if (replacementValue == null) {
                    return fv;
                }

                return new StandardFieldValue(replacementValue, fv.getField(), fv.getParent().orElse(null));
            });
    }

}
