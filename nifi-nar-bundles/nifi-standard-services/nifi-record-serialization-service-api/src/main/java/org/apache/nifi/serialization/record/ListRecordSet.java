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

package org.apache.nifi.serialization.record;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ListRecordSet implements RecordSet {
    private final Iterator<Record> recordItr;
    private final RecordSchema schema;

    public ListRecordSet(final RecordSchema schema, final List<Record> records) {
        this.schema = schema;

        final List<Record> copy = new ArrayList<>(records);
        recordItr = copy.iterator();
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    @Override
    public Record next() {
        return recordItr.hasNext() ? recordItr.next() : null;
    }
}
