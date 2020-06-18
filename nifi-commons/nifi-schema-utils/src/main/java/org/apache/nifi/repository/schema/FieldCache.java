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

package org.apache.nifi.repository.schema;

/**
 * <p>
 *     For many write-ahead logs, the keys and values that are stored for fields are very repetitive. As a result, it is common when reading data from the repository
 *     to read the same value over and over, but each time the value is read, the data is a separate object in memory.
 * </p>
 *
 * <p>
 *     Take, for example, the case when the value "Hello World" is stored as a field in nearly every record that is written to the WAL. When the record is created,
 *     it may be created in such a way that a single String is referenced over and over again. However, when the Write-Ahead Log is restored, each time that value is encountered,
 *     a new String must be created because it is being deserialized from an InputStream. So instead of a single String occupying approximately 25 bytes of heap, if this is encountered
 *     1 million times, the result is that 1 million 25-byte Strings remain on the heap, totaling about 25 MB of heap space.
 * </p>
 *
 * <p>
 *     In order to avoid this, a FieldValueCache can be provided to the SerDe. As a result, whenever a value is read, that value is added to a cache as the "canonical representation" of that
 *     value. The next time that value is encountered, if the first instance is still available in the cache, the canonical representation will be returned. As a result, we end up creating the
 *     first String with the value of "Hello World" and then the second instance. The second instance is then used to lookup the canonical representation (the first instance) and the canonical
 *     representation is then included in the record. The second instance is then garbage collected. As a result, even with millions of records having the value "Hello World" only a single
 *     instance needs to be kept in heap.
 * </p>
 */
public interface FieldCache {

    /**
     * Check if the given value already exists in the cache and if so returns it. If the value does not
     * already exist in the cache, adds the given value to the cache, evicting an existing entry(ies) if necessary
     * @param value the value to cache
     * @return the canonical representation of the value that should be used
     */
    String cache(String value);

    /**
     * Clears the cache
     */
    void clear();
}
