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
package org.apache.nifi.registry.db.entity;

public class FlowEntity extends BucketItemEntity {

    private long snapshotCount;

    public FlowEntity() {
        setType(BucketItemEntityType.FLOW);
    }

    public long getSnapshotCount() {
        return snapshotCount;
    }

    public void setSnapshotCount(long snapshotCount) {
        this.snapshotCount = snapshotCount;
    }

    @Override
    public void setType(BucketItemEntityType type) {
        if (BucketItemEntityType.FLOW != type) {
            throw new IllegalStateException("Must set type to FLOW");
        }
        super.setType(type);
    }
}
