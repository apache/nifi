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
package org.apache.nifi.cdc.mongodb.processors;

final class StateKeys {

    /** The {@code _data} string of the resume token of the last change event written to a committed FlowFile. */
    static final String RESUME_TOKEN = "resume.token";

    /** What the stored resume token belongs to, so that it is never used for a different scope; see StreamOptions. */
    static final String STREAM_SOURCE = "stream.source";

    /** Set once the initial snapshot has read the whole collection. */
    static final String SNAPSHOT_DONE = "snapshot.done";

    /** The identifier of the last document the snapshot wrote, as Extended JSON, so that it can carry on after it. */
    static final String SNAPSHOT_LAST_ID = "snapshot.last.id";

    /** The cluster time the snapshot is consistent with; the change stream carries on from there. */
    static final String SNAPSHOT_START_TIME = "snapshot.start.time";

    private StateKeys() {
    }
}
