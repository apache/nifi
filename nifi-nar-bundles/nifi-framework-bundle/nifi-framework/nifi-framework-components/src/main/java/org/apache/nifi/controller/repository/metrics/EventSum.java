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

package org.apache.nifi.controller.repository.metrics;

import org.apache.nifi.controller.repository.FlowFileEvent;

import java.util.concurrent.atomic.AtomicReference;

public class EventSum {
    private final AtomicReference<EventSumValue> ref = new AtomicReference<>();

    public EventSumValue getValue() {
        final EventSumValue value = ref.get();
        return value == null ? new EventSumValue(System.currentTimeMillis()) : value;
    }

    public EventSumValue addOrReset(final FlowFileEvent event, final long timestamp) {
        final long expectedSecond = timestamp / 1000;

        EventSumValue curValue;
        while (true) {
            curValue = ref.get();
            if (curValue == null || (curValue.getTimestamp() / 1000) != expectedSecond) {
                final EventSumValue newValue = new EventSumValue(timestamp);
                final boolean replaced = ref.compareAndSet(curValue, newValue);
                if (replaced) {
                    newValue.add(event);
                    return curValue;
                }
            } else {
                break;
            }
        }

        curValue.add(event);
        return null;
    }


    public EventSumValue reset(final long ifOlderThan) {
        while (true) {
            final EventSumValue curValue = ref.get();
            if (curValue == null) {
                return null;
            }

            if (curValue.getTimestamp() < ifOlderThan) {
                if (ref.compareAndSet(curValue, null)) {
                    return curValue;
                }
            } else {
                return null;
            }
        }
    }
}
