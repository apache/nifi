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

import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.SmbConfig;
import org.apache.nifi.context.PropertyContext;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.nifi.smb.common.SmbProperties.ENABLE_DFS;
import static org.apache.nifi.smb.common.SmbProperties.SMB_DIALECT;
import static org.apache.nifi.smb.common.SmbProperties.TIMEOUT;
import static org.apache.nifi.smb.common.SmbProperties.USE_ENCRYPTION;

public final class SmbUtils {

    private SmbUtils() {
        // util class' constructor
    }

    public static SMBClient buildSmbClient(final PropertyContext context) {
        return SmbClient.create(buildSmbConfig(context));
    }

    static SmbConfig buildSmbConfig(final PropertyContext context) {
        final SmbConfig.Builder configBuilder = SmbConfig.builder();

        if (context.getProperty(SMB_DIALECT).isSet()) {
            final SmbDialect dialect = SmbDialect.valueOf(context.getProperty(SMB_DIALECT).getValue());

            if (dialect != SmbDialect.AUTO) {
                configBuilder.withDialects(dialect.getSmbjDialect());
            }
        }

        if (context.getProperty(USE_ENCRYPTION).isSet()) {
            configBuilder.withEncryptData(context.getProperty(USE_ENCRYPTION).asBoolean());
        }

        if (context.getProperty(ENABLE_DFS).isSet()) {
            configBuilder.withDfsEnabled(context.getProperty(ENABLE_DFS).asBoolean());
        }

        if (context.getProperty(TIMEOUT).isSet()) {
            configBuilder.withTimeout(context.getProperty(TIMEOUT).asTimePeriod(MILLISECONDS), MILLISECONDS);
        }

        return configBuilder.build();
    }
}
