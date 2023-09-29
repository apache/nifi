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

package org.apache.nifi.sql.internal;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.sql.ResettableDataSource;
import org.apache.nifi.sql.RowStream;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class NiFiTableEnumerator implements Enumerator<Object> {
    private final ResettableDataSource dataSource;
    private final ComponentLog logger;
    private final int[] fields;
    private final Runnable onFinishCallback;
    private final Consumer<NiFiTableEnumerator> onCloseCallback;


    private RowStream rowStream;
    private Object currentRow;
    private int recordsRead = 0;


    public NiFiTableEnumerator(final ResettableDataSource dataSource, final ComponentLog logger, final int[] fields, final Runnable onFinishCallback,
                               final Consumer<NiFiTableEnumerator> onCloseCallback) {
        this.dataSource = dataSource;
        this.logger = logger;
        this.fields = fields;
        this.onFinishCallback = onFinishCallback;
        this.onCloseCallback = onCloseCallback;
        reset();
    }


    @Override
    public Object current() {
        return currentRow;
    }


    @Override
    public boolean moveNext() {
        currentRow = null;
        try {
            final Object[] row = rowStream.nextRow();
            if (row == null) {
                // If we are out of data, close the InputStream. We do this because
                // Calcite does not necessarily call our close() method.
                close();

                try {
                    onFinish();
                } catch (final Exception e) {
                    logger.error("Failed to perform tasks when enumerator was finished", e);
                }

                return false;
            }

            currentRow = filterColumns(row);
        } catch (final Exception e) {
            throw new ProcessException("Failed to read next row in stream", e);
        }

        recordsRead++;
        return true;
    }

    public int getRecordsRead() {
        return recordsRead;
    }

    private void onFinish() {
        if (onFinishCallback != null) {
            onFinishCallback.run();
        }
    }

    private Object filterColumns(final Object[] row) {
        if (row == null) {
            return null;
        }

        // If we want no fields or if the row is null, just return null
        if (fields == null) {
            return row;
        }

        // If we want only a single field, then Calcite is going to expect us to return
        // the actual value, NOT a 1-element array of values.
        if (fields.length == 1) {
            final int desiredCellIndex = fields[0];
            return cast(row[desiredCellIndex]);
        }

        // Create a new Object array that contains only the desired fields.
        final Object[] filtered = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            final int indexToKeep = fields[i];
            filtered[i] = cast(row[indexToKeep]);
        }

        return filtered;
    }

    private Object cast(final Object toCast) {
        if (toCast == null) {
            return null;
        } else if (toCast.getClass().isArray()) {
            final List<Object> list = new ArrayList<>(Array.getLength(toCast));
            for (int i = 0; i < Array.getLength(toCast); i++) {
                list.add(Array.get(toCast, i));
            }
            return list;
        } else {
            return toCast;
        }
    }

    @Override
    public void reset() {
        if (rowStream != null) {
            try {
                rowStream.close();
            } catch (final Exception e) {
                logger.warn("Could not close data stream {}", rowStream, e);
            }
        }

        try {
            rowStream = dataSource.reset();
        } catch (final Exception e) {
            throw new RuntimeException("Failed to data stream from " + dataSource, e);
        }
    }


    @Override
    public final void close() {
        try {
            if (onCloseCallback != null) {
                onCloseCallback.accept(this);
            }
        } finally {
            if (rowStream != null) {
                try {
                    rowStream.close();
                } catch (Exception e) {
                    logger.warn("Failed to close {}", rowStream, e);
                }
            }
        }
    }
}
