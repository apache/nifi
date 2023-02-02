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
package org.apache.nifi.smb.common;

import com.hierynomus.mssmb2.SMB2Dialect;
import org.apache.nifi.components.DescribedValue;

public enum SmbDialect implements DescribedValue {

    AUTO("AUTO", null),
    SMB_2_0_2("SMB 2.0.2", SMB2Dialect.SMB_2_0_2),
    SMB_2_1("SMB 2.1", SMB2Dialect.SMB_2_1),
    SMB_3_0("SMB 3.0", SMB2Dialect.SMB_3_0),
    SMB_3_0_2("SMB 3.0.2", SMB2Dialect.SMB_3_0_2),
    SMB_3_1_1("SMB 3.1.1", SMB2Dialect.SMB_3_1_1);

    private final String displayName;

    private final SMB2Dialect smbjDialect;

    SmbDialect(String displayName, SMB2Dialect smbjDialect) {
        this.displayName = displayName;
        this.smbjDialect = smbjDialect;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return null;
    }

    public SMB2Dialect getSmbjDialect() {
        return smbjDialect;
    }
}
