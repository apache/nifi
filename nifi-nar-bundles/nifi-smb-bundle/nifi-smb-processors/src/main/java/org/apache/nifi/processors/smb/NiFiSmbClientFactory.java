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
package org.apache.nifi.processors.smb;

import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.Share;
import java.io.IOException;

public class NiFiSmbClientFactory {

    NiFiSmbClient create(String hostName, Integer port, String shareName,
            AuthenticationContext authenticationContext, SMBClient smbClient) throws IOException {
        final Connection connection = smbClient.connect(hostName, port);
        final Share share = connection.authenticate(authenticationContext).connectShare(shareName);
        if (share instanceof DiskShare) {
            return new NiFiSmbClient(connection, (DiskShare) share);
        } else {
            throw new IllegalArgumentException("NiFi supports only disk shares but " +
                    share.getClass().getSimpleName() + " found on host " + hostName + "!");
        }
    }

}
