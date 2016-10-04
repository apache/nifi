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

public class CountSizeEntityAccess implements EntityAccess<TimedCountSize> {
    @Override
    public TimedCountSize aggregate(final TimedCountSize oldValue, final TimedCountSize toAdd) {
        if (oldValue == null && toAdd == null) {
            return new TimedCountSize(0L, 0L);
        } else if (oldValue == null) {
            return toAdd;
        } else if (toAdd == null) {
            return oldValue;
        }

        return new TimedCountSize(oldValue.getCount() + toAdd.getCount(), oldValue.getSize() + toAdd.getSize());
    }

    @Override
    public TimedCountSize createNew() {
        return new TimedCountSize(0L, 0L);
    }

    @Override
    public long getTimestamp(final TimedCountSize entity) {
        return entity == null ? 0L : entity.getTimestamp();
    }
}
