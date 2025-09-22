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

package org.apache.nifi.minifi.commons.status.processor;

import org.apache.nifi.minifi.commons.status.common.ValidationError;

import java.util.List;

public class ProcessorHealth implements java.io.Serializable {
    private String runStatus;
    private boolean hasBulletins;
    private List<ValidationError> validationErrorList;

    public boolean isHasBulletins() {
        return hasBulletins;
    }

    public void setHasBulletins(boolean hasBulletins) {
        this.hasBulletins = hasBulletins;
    }

    public String getRunStatus() {
        return runStatus;
    }

    public void setRunStatus(String runStatus) {
        this.runStatus = runStatus;
    }

    public List<ValidationError> getValidationErrorList() {
        return validationErrorList;
    }

    public void setValidationErrorList(List<ValidationError> validationErrorList) {
        this.validationErrorList = validationErrorList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProcessorHealth that = (ProcessorHealth) o;

        if (isHasBulletins() != that.isHasBulletins()) return false;
        if (getRunStatus() != null ? !getRunStatus().equals(that.getRunStatus()) : that.getRunStatus() != null) return false;
        return getValidationErrorList() != null ? getValidationErrorList().equals(that.getValidationErrorList()) : that.getValidationErrorList() == null;

    }

    @Override
    public int hashCode() {
        int result = getRunStatus() != null ? getRunStatus().hashCode() : 0;
        result = 31 * result + (isHasBulletins() ? 1 : 0);
        result = 31 * result + (getValidationErrorList() != null ? getValidationErrorList().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "runStatus='" + runStatus + '\'' +
                ", hasBulletins=" + hasBulletins +
                ", validationErrorList=" + validationErrorList +
                '}';
    }
}
