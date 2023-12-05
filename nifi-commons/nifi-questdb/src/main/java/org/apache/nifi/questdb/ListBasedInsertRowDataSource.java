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
import java.util.List;

final class ListBasedInsertRowDataSource extends FillerBasedInsertRowDataSource {
    private final List<List<Object>> rowData;
    private final Instant timestamp;
    private int actualRow = 0;

    ListBasedInsertRowDataSource(final List<List<Object>> rowData, final Instant timestamp) {
        this.rowData = rowData;
        this.timestamp = timestamp;
    }

    @Override
    public boolean hasNextToInsert() {
        return false;
    }

    @Override
    public void fillRowData(final InsertRowContext context) {
        context.initializeRow(timestamp);
        final List<Object> row = rowData.get(actualRow);

        for (int position = 0; position < row.size(); position++) {
            final Object fieldData = row.get(position);
            getFillerForFieldType(fieldData.getClass()).accept(position, fieldData, context);
        }

        actualRow++;
    }
}
