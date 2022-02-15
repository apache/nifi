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

package org.apache.nifi.processor.util.list;

import java.util.function.Function;

public class ListableEntityWrapper<T> implements ListableEntity {

    private T rawEntity;
    private final Function<T, String> toName;
    private final Function<T, String> toIdentifier;
    private final Function<T, Long> toTimestamp;
    private final Function<T, Long> toSize;

    public ListableEntityWrapper(
        T rawEntity,
        Function<T, String> toName,
        Function<T, String> toIdentifier,
        Function<T, Long> toTimestamp,
        Function<T, Long> toSize
    ) {
        this.rawEntity = rawEntity;
        this.toName = toName;
        this.toIdentifier = toIdentifier;
        this.toTimestamp = toTimestamp;
        this.toSize = toSize;
    }

    public T getRawEntity() {
        return rawEntity;
    }

    @Override
    public String getName() {
        return toName.apply(rawEntity);
    }

    @Override
    public String getIdentifier() {
        return toIdentifier.apply(rawEntity);
    }

    @Override
    public long getTimestamp() {
        return toTimestamp.apply(rawEntity);
    }

    @Override
    public long getSize() {
        return toSize.apply(rawEntity);
    }
}
