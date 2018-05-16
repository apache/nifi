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

import java.util.stream.Stream;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.filter.RecordPathFilter;

public class PredicatePath extends RecordPathSegment {
    private final RecordPathFilter filter;

    public PredicatePath(final RecordPathSegment parent, final RecordPathFilter filter, final boolean absolute) {
        super("[" + filter + "]", parent, absolute);
        this.filter = filter;
    }

    @Override
    public Stream<FieldValue> evaluate(final RecordPathEvaluationContext context) {
        final Stream<FieldValue> valueStream = getParentPath().evaluate(context);

        return valueStream.flatMap(fieldVal -> {
            // For the duration of this Predicate, we want to consider the 'context node' to be
            // whatever value is given to us in the field value. We then want to return the 'context node'
            // back to what it was before this Predicate.
            final FieldValue previousContextNode = context.getContextNode();
            context.setContextNode(fieldVal);
            try {
                // Really what we want to do is filter out Stream<FieldValue> but that becomes very difficult
                // to implement for the RecordPathFilter's. So, instead, we pass
                // the RecordPathEvaluationContext and receive back a Stream<FieldValue>. Since this is a Predicate,
                // though, we don't want to transform our Stream - we just want to filter it. So we handle this by
                // mapping the result back to fieldVal. And since this predicate shouldn't return the same field multiple
                // times, we will limit the stream to 1 element.
                return filter.filter(context, false)
                    .limit(1)
                    .map(ignore -> fieldVal);
            } finally {
                context.setContextNode(previousContextNode);
            }
        });
    }
}
