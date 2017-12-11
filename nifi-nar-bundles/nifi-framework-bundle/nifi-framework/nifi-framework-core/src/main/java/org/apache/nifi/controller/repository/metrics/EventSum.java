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

import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.controller.repository.FlowFileEvent;

public class EventSum {

    private final AtomicReference<EventSumValue> ref = new AtomicReference<>();

    public EventSumValue getValue() {
        final EventSumValue value = ref.get();
        return value == null ? new EventSumValue() : value;
    }

    public void addOrReset(final FlowFileEvent event) {
        final long expectedMinute = System.currentTimeMillis() / 60000;

        EventSumValue curValue;
        while (true) {
            curValue = ref.get();
            if (curValue == null || curValue.getMinuteTimestamp() != expectedMinute) {
                final EventSumValue newValue = new EventSumValue();
                final boolean replaced = ref.compareAndSet(curValue, newValue);
                if (replaced) {
                    curValue = newValue;
                    break;
                }
            } else {
                break;
            }
        }

        curValue.add(event);
    }
}
