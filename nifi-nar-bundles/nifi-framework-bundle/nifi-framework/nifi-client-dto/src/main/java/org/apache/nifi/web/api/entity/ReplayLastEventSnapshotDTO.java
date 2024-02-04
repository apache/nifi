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
package org.apache.nifi.web.api.entity;

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.xml.bind.annotation.XmlRootElement;
import java.util.Collection;

@XmlRootElement(name = "replayLastEventSnapshot")
public class ReplayLastEventSnapshotDTO {
    private Collection<Long> eventsReplayed;
    private String failureExplanation;
    private Boolean eventAvailable;

    @Schema(description = "The IDs of the events that were successfully replayed")
    public Collection<Long> getEventsReplayed() {
        return eventsReplayed;
    }

    public void setEventsReplayed(final Collection<Long> eventsReplayed) {
        this.eventsReplayed = eventsReplayed;
    }

    @Schema(description = "If unable to replay an event, specifies why the event could not be replayed")
    public String getFailureExplanation() {
        return failureExplanation;
    }

    public void setFailureExplanation(final String failureExplanation) {
        this.failureExplanation = failureExplanation;
    }

    @Schema(description = "Whether or not an event was available. This may not be populated if there was a failure.")
    public Boolean getEventAvailable() {
        return eventAvailable;
    }

    public void setEventAvailable(final Boolean eventAvailable) {
        this.eventAvailable = eventAvailable;
    }
}
