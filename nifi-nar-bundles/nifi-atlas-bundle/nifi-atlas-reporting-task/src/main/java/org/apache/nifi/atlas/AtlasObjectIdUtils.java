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
package org.apache.nifi.atlas;

import org.apache.atlas.model.instance.AtlasObjectId;

import java.util.Objects;
import java.util.function.Predicate;

import static org.apache.nifi.atlas.processors.ExpressionUtils.isExpressionLanguagePresent;

public class AtlasObjectIdUtils {

    // Condition of a valid AtlasObjectId:
    // It has a GUID, or It has a type name and unique attributes, and none of attribute contains ExpressionLanguage.
    public static final Predicate<String> notEmpty = s -> s != null && !s.isEmpty();
    public static final Predicate<AtlasObjectId> validObjectId = id -> notEmpty.test(id.getGuid())
            || (notEmpty.test(id.getTypeName())
                && (id.getUniqueAttributes() != null
                    && !id.getUniqueAttributes().isEmpty()
                    && id.getUniqueAttributes().values().stream().filter(Objects::nonNull)
                        .noneMatch(o -> isExpressionLanguagePresent(o.toString()))));

}
