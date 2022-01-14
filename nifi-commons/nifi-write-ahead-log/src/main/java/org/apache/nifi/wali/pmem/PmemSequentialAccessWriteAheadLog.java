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
package org.apache.nifi.wali.pmem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.nifi.wali.ByteArrayDataOutputStream;
import org.apache.nifi.wali.ObjectPool;
import org.apache.nifi.wali.SequentialAccessWriteAheadLog;
import org.apache.nifi.wali.WriteAheadJournal;
import org.wali.SerDeFactory;
import org.wali.SyncListener;

import java.io.File;
import java.io.IOException;

public class PmemSequentialAccessWriteAheadLog<T> extends SequentialAccessWriteAheadLog<T> {
    private static final Logger logger = LoggerFactory.getLogger(PmemSequentialAccessWriteAheadLog.class);

    public PmemSequentialAccessWriteAheadLog(final File storageDirectory, final SerDeFactory<T> serdeFactory) throws IOException {
        super(storageDirectory, serdeFactory);
    }

    public PmemSequentialAccessWriteAheadLog(final File storageDirectory, final SerDeFactory<T> serdeFactory, final SyncListener syncListener) throws IOException {
        super(storageDirectory, serdeFactory, syncListener);
    }

    @Override
    protected WriteAheadJournal<T> newWriteAheadJournal(File journalFile, SerDeFactory<T> serdeFactory, ObjectPool<ByteArrayDataOutputStream> streamPool, long initialTransactionId) {
        // DO NOT call super.newWriteAheadJournal()

        return new PmemLengthDelimitedJournal<T>(journalFile, serdeFactory, streamPool, initialTransactionId);
    }
}
