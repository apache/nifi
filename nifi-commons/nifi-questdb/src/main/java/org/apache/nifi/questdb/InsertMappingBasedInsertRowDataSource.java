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
package org.apache.nifi.questdb;

import java.time.Instant;
import java.util.Iterator;

final class InsertMappingBasedInsertRowDataSource<T> extends FillerBasedInsertRowDataSource {
    private final InsertMapping<T> mapping;
    private final Iterator<T> insertables;

    InsertMappingBasedInsertRowDataSource(final InsertMapping<T> insertMapping, final Iterable<T> insertables) {
        this.mapping = insertMapping;
        this.insertables = insertables.iterator();
    }

    @Override
    public boolean hasNextToInsert() {
        return insertables.hasNext();
    }

    @Override
    public void fillRowData(final InsertRowContext context) {
        final T instertable = insertables.next();
        context.initializeRow((Instant) mapping.getMappingAt(mapping.getTimestampPosition()).apply(instertable));

        for (int position = 0; position < mapping.getNumberOfFields(); position++) {
            if (mapping.getTimestampPosition() == position) {
                continue;
            }

            getFillerForFieldType(mapping.getFieldTypeAt(position)).accept(position, mapping.getMappingAt(position).apply(instertable), context);
        }
    }
}
