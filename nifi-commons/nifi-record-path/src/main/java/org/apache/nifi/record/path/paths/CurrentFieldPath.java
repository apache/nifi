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

public class CurrentFieldPath extends RecordPathSegment {

    public CurrentFieldPath(final RecordPathSegment parentPath, final boolean absolute) {
        super(parentPath == null ? "." : parentPath.getPath() + "/.", parentPath, absolute);
    }

    @Override
    public Stream<FieldValue> evaluate(final RecordPathEvaluationContext context) {
        final FieldValue contextNode = context.getContextNode();
        if (contextNode != null) {
            return Stream.of(contextNode);
        }

        final RecordPathSegment parentPath = getParentPath();
        if (parentPath == null) {
            return Stream.of(context.getContextNode());
        } else {
            return parentPath.evaluate(context);
        }
    }

}
