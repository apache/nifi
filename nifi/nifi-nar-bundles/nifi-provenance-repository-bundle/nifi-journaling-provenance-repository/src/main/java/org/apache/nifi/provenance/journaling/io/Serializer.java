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
package org.apache.nifi.provenance.journaling.io;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.nifi.provenance.ProvenanceEventRecord;

public interface Serializer {

    /**
     * Returns the serialization version that is used to serialize records
     * @return
     */
    int getVersion();
    
    /**
     * Returns the name of the codec used to serialize the records
     * @return
     */
    String getCodecName();
    
    /**
     * Serializes the given even to the given DataOutputStream.
     * This method should NOT serialize the ID, as the ID is not yet known. The ID will instead by
     * serialized to the stream appropriately by the JournalWriter.
     * 
     * @param event
     * @param out
     * @throws IOException
     */
    void serialize(ProvenanceEventRecord event, DataOutputStream out) throws IOException;
    
}
