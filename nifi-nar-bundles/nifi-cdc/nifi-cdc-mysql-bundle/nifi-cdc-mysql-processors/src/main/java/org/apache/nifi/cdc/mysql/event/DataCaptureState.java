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
package org.apache.nifi.cdc.mysql.event;

public class DataCaptureState {

    private String binlogFile = null;
    private long binlogPosition = 4;

    private boolean useGtid = false;
    private String gtidSet = null;
    private long sequenceId = 0L;

    public DataCaptureState() {
    }

    public DataCaptureState(String binlogFile, long binlogPosition, boolean useGtid, String gtidSet, long sequenceId) {
        this.binlogFile = binlogFile;
        this.binlogPosition = binlogPosition;
        this.useGtid = useGtid;
        this.gtidSet = gtidSet;
        this.sequenceId = sequenceId;
    }

    public String getBinlogFile() {
        return binlogFile;
    }

    public void setBinlogFile(String binlogFile) {
        this.binlogFile = binlogFile;
    }

    public long getBinlogPosition() {
        return binlogPosition;
    }

    public void setBinlogPosition(long binlogPosition) {
        this.binlogPosition = binlogPosition;
    }

    public boolean isUseGtid() {
        return useGtid;
    }

    public void setUseGtid(boolean useGtid) {
        this.useGtid = useGtid;
    }

    public String getGtidSet() {
        return gtidSet;
    }

    public void setGtidSet(String gtidSet) {
        this.gtidSet = gtidSet;
    }

    public long getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(long sequenceId) {
        this.sequenceId = sequenceId;
    }

    public DataCaptureState copy() {
        return new DataCaptureState(binlogFile, binlogPosition, useGtid, gtidSet, sequenceId);
    }
}
