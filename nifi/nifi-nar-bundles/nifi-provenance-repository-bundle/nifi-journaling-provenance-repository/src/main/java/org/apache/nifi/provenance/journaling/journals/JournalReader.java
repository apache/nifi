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
package org.apache.nifi.provenance.journaling.journals;

import java.io.Closeable;
import java.io.IOException;

import org.apache.nifi.provenance.ProvenanceEventRecord;

public interface JournalReader extends Closeable {

    /**
     * Retrieve a specific event from the journal, given the offset of the Block and the ID of the event
     * @param blockOffset
     * @param eventId
     * @return
     * @throws IOException
     */
    ProvenanceEventRecord getEvent(long blockOffset, long eventId) throws IOException;
    
    /**
     * Retrieve the next event in the journal, or <code>null</code> if no more events exist
     * @return
     * @throws IOException
     */
    ProvenanceEventRecord nextEvent() throws IOException;

    /**
     * Returns the current byte offset into the Journal from which the next event (if any) will be read
     * @return
     */
    long getPosition();
}
