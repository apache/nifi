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

package org.apache.nifi.toolkit.admin.util

import org.apache.commons.lang3.StringUtils

class Version {

    private String[] versionNumber
    private String delimeter

    Version(String version, String delimeter) {
        this.versionNumber = version.tokenize(delimeter)
        this.delimeter = delimeter
    }

    String[] getVersionNumber() {
        return versionNumber
    }

    void setVersionNumber(String[] versionNumber) {
        this.versionNumber = versionNumber
    }

    String getDelimeter() {
        return delimeter
    }

    void setDelimeter(String delimeter) {
        this.delimeter = delimeter
    }

    boolean equals(o) {
        if (this.is(o)) return true
        if (getClass() != o.class) return false
        Version version = (Version) o
        if (!Arrays.equals(versionNumber, version.versionNumber)) return false
        return true
    }

    int hashCode() {
        return (versionNumber != null ? Arrays.hashCode(versionNumber) : 0)
    }

    public final static Comparator<Version> VERSION_COMPARATOR = new Comparator<Version>() {
        @Override
        int compare(Version o1, Version o2) {
            String[] o1V = o1.versionNumber
            String[] o2V = o2.versionNumber

            for(int i = 0; i < o1V.length; i++) {

                if(o2V.length == i ){
                    return 1
                }else {
                    Integer val1 = Integer.parseInt(o1V[i])
                    Integer val2 = Integer.parseInt(o2V[i])
                    if (val1.compareTo(val2) != 0) {
                        return val1.compareTo(val2)
                    }
                }

            }
            return 0
        }
    }


    @Override
    public String toString() {
        StringUtils.join(versionNumber,delimeter)
    }
}
