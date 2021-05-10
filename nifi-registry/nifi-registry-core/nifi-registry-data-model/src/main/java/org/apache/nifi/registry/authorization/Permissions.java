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
package org.apache.nifi.registry.authorization;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel
public class Permissions {

    private boolean canRead = false;
    private boolean canWrite = false;
    private boolean canDelete = false;

    public Permissions() {
    }

    public Permissions(Permissions permissions) {
        if (permissions == null) {
            throw new IllegalArgumentException("Cannot call copy constructor with null argument");
        }

        this.canRead = permissions.getCanRead();
        this.canWrite = permissions.getCanWrite();
        this.canDelete = permissions.getCanDelete();
    }

    /**
     * @return Indicates whether the user can read a given resource.
     */
    @ApiModelProperty(
            value = "Indicates whether the user can read a given resource.",
            readOnly = true
    )
    public boolean getCanRead() {
        return canRead;
    }

    public void setCanRead(boolean canRead) {
        this.canRead = canRead;
    }

    public Permissions withCanRead(boolean canRead) {
        setCanRead(canRead);
        return this;
    }

    /**
     * @return Indicates whether the user can write a given resource.
     */
    @ApiModelProperty(
            value = "Indicates whether the user can write a given resource.",
            readOnly = true
    )
    public boolean getCanWrite() {
        return canWrite;
    }

    public void setCanWrite(boolean canWrite) {
        this.canWrite = canWrite;
    }

    public Permissions withCanWrite(boolean canWrite) {
        setCanWrite(canWrite);
        return this;
    }

    /**
     * @return Indicates whether the user can delete a given resource.
     */
    @ApiModelProperty(
            value = "Indicates whether the user can delete a given resource.",
            readOnly = true
    )
    public boolean getCanDelete() {
        return canDelete;
    }

    public void setCanDelete(boolean canDelete) {
        this.canDelete = canDelete;
    }

    public Permissions withCanDelete(boolean canDelete) {
        setCanDelete(canDelete);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Permissions that = (Permissions) o;

        if (canRead != that.canRead) return false;
        if (canWrite != that.canWrite) return false;
        return canDelete == that.canDelete;
    }

    @Override
    public int hashCode() {
        int result = (canRead ? 1 : 0);
        result = 31 * result + (canWrite ? 1 : 0);
        result = 31 * result + (canDelete ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Permissions{" +
                "canRead=" + canRead +
                ", canWrite=" + canWrite +
                ", canDelete=" + canDelete +
                '}';
    }
}
