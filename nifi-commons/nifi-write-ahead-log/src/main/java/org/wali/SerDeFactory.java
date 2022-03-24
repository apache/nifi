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

package org.wali;

public interface SerDeFactory<T> {

    /**
     * Returns a new SerDe
     *
     * @param encodingName the name of encoding that was used when writing the serialized data, or <code>null</code> if
     *            the SerDe is to be used for serialization purposes
     * @return a SerDe
     */
    SerDe<T> createSerDe(String encodingName);

    /**
     * Returns the unique ID for the given record
     *
     * @param record to obtain identifier for
     * @return identifier of record
     */
    Object getRecordIdentifier(T record);

    /**
     * Returns the UpdateType for the given record
     *
     * @param record to retrieve update type for
     * @return update type
     */
    UpdateType getUpdateType(T record);

    /**
     * Returns the external location of the given record; this is used when a
     * record is moved away from WALI or is being re-introduced to WALI. For
     * example, WALI can be updated with a record of type
     * {@link UpdateType#SWAP_OUT} that indicates a Location of
     * file://tmp/external1 and can then be re-introduced to WALI by updating
     * WALI with a record of type {@link UpdateType#CREATE} that indicates a
     * Location of file://tmp/external1
     *
     * @param record to get location of
     * @return location
     */
    String getLocation(T record);
}
