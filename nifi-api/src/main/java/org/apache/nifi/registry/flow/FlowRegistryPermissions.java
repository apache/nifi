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
package org.apache.nifi.registry.flow;

public class FlowRegistryPermissions {
    private boolean canRead;
    private boolean canWrite;
    private boolean canDelete;

    public boolean getCanRead() {
        return canRead;
    }

    public boolean getCanWrite() {
        return canWrite;
    }

    public boolean getCanDelete() {
        return canDelete;
    }

    public void setCanRead(final boolean canRead) {
        this.canRead = canRead;
    }

    public void setCanWrite(final boolean canWrite) {
        this.canWrite = canWrite;
    }

    public void setCanDelete(final boolean canDelete) {
        this.canDelete = canDelete;
    }
}
