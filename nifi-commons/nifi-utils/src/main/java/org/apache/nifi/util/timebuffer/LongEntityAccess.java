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
package org.apache.nifi.util.timebuffer;

import java.util.function.LongSupplier;

public class LongEntityAccess implements EntityAccess<TimestampedLong> {

    private final LongSupplier currentTimeSupplier;

    public LongEntityAccess() {
        this(System::currentTimeMillis);
    }

    public LongEntityAccess(final LongSupplier currentTimeSupplier) {
        this.currentTimeSupplier = currentTimeSupplier;
    }

    @Override
    public TimestampedLong aggregate(TimestampedLong oldValue, TimestampedLong toAdd) {
        if (oldValue == null && toAdd == null) {
            return new TimestampedLong(0L, currentTimeSupplier.getAsLong());
        } else if (oldValue == null) {
            return toAdd;
        } else if (toAdd == null) {
            return oldValue;
        }

        return new TimestampedLong(oldValue.getValue() + toAdd.getValue(), currentTimeSupplier.getAsLong());
    }

    @Override
    public TimestampedLong createNew() {
        return new TimestampedLong(0L, currentTimeSupplier.getAsLong());
    }

    @Override
    public long getTimestamp(TimestampedLong entity) {
        return entity == null ? 0L : entity.getTimestamp();
    }
}
