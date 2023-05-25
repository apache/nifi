/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
*/
package org.apache.nifi.remote.io.socket;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;

public class NetworkUtils {

    /**
     * Get Interface Address using interface name eg. en0, eth0
     *
     * @param interfaceName Network Interface Name
     * @return Interface Address or null when matching network interface name not found
     * @throws SocketException Thrown when failing to get interface addresses
     */
    public static InetAddress getInterfaceAddress(final String interfaceName) throws SocketException {
        InetAddress interfaceAddress = null;
        if (interfaceName != null && !interfaceName.isEmpty()) {
            NetworkInterface networkInterface = NetworkInterface.getByName(interfaceName);
            interfaceAddress = networkInterface.getInetAddresses().nextElement();
        }
        return interfaceAddress;
    }

}
