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

public class StandardJournalRecovery implements JournalRecovery {
    private final int updateCount;
    private final long maxTransactionId;
    private final boolean eofException;

    public StandardJournalRecovery(final int updateCount, final long maxTransactionId, final boolean eofException) {
        this.updateCount = updateCount;
        this.maxTransactionId = maxTransactionId;
        this.eofException = eofException;
    }

    @Override
    public int getUpdateCount() {
        return updateCount;
    }

    @Override
    public long getMaxTransactionId() {
        return maxTransactionId;
    }

    @Override
    public boolean isEOFExceptionEncountered() {
        return eofException;
    }
}
