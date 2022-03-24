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
package org.apache.nifi.hbase.scan;

import java.util.Arrays;

/**
 * Wrapper to encapsulate a column family and qualifier.
 */
public class Column {

    private final byte[] family;
    private final byte[] qualifier;

    public Column(byte[] family, byte[] qualifier) {
        this.family = family;
        this.qualifier = qualifier;
    }

    public byte[] getFamily() {
        return family;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Column)) {
            return false;
        }

        final Column other = (Column) obj;
        return ((this.family == null && other.family == null)
                || (this.family != null && other.family != null && Arrays.equals(this.family, other.family)))
                && ((this.qualifier == null && other.qualifier == null)
                || (this.qualifier != null && other.qualifier != null && Arrays.equals(this.qualifier, other.qualifier)));
    }

    @Override
    public int hashCode() {
        int result = 37;
        if (family != null) {
            for (byte b : family) {
                result += (int)b;
            }
        }
        if (qualifier != null) {
            for (byte b : qualifier) {
                result += (int)b;
            }
        }
        return result;
    }

}
