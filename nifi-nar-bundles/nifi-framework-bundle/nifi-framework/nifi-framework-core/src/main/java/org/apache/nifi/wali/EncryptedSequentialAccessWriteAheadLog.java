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

package org.apache.nifi.wali;

import java.io.File;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wali.SerDeFactory;
import org.wali.SyncListener;

/**
 * <p>
 * This implementation of {@link org.wali.WriteAheadRepository} is just a marker implementation wrapping the {@link SequentialAccessWriteAheadLog}. It exists to allow
 * users to configure  {@code nifi.properties} with
 * {@code nifi.flowfile.repository.wal.implementation=org.apache.nifi.wali.EncryptedSequentialAccessWriteAheadLog}
 * because the {@link org.wali.SerDe} interface is not exposed at that level. By selecting
 * this WAL implementation, the admin is enabling the encrypted flowfile repository, but all
 * other behavior is identical.
 * </p>
 *
 * <p>
 * This implementation transparently encrypts the objects as they are persisted to the journal file.
 * </p>
 */
public class EncryptedSequentialAccessWriteAheadLog<T> extends SequentialAccessWriteAheadLog<T> {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedSequentialAccessWriteAheadLog.class);


    public EncryptedSequentialAccessWriteAheadLog(final File storageDirectory, final SerDeFactory<T> serdeFactory) throws IOException {
        this(storageDirectory, serdeFactory, SyncListener.NOP_SYNC_LISTENER);
    }

    public EncryptedSequentialAccessWriteAheadLog(final File storageDirectory, final SerDeFactory<T> serdeFactory, final SyncListener syncListener) throws IOException {
        super(storageDirectory, serdeFactory, syncListener);
    }
}
