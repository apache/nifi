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
package org.apache.nifi.cdc.event.io;

import org.apache.nifi.components.DescribedValue;

public enum FlowFileEventWriteStrategy implements DescribedValue {
    MAX_EVENTS_PER_FLOWFILE(
            "Max Events Per FlowFile",
            "This strategy causes at most the number of events specified in the 'Number of Events Per FlowFile' property to be written per FlowFile. If the processor is stopped before the "
                    + "specified number of events has been written (or the event queue becomes empty), the fewer number of events will still be written as a FlowFile before stopping."
    ),
    ONE_TRANSACTION_PER_FLOWFILE(
            "One Transaction Per FlowFile",
            "This strategy causes each event from a transaction (from BEGIN to COMMIT) to be written to a FlowFile"
    );

    private String displayName;
    private String description;

    FlowFileEventWriteStrategy(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
