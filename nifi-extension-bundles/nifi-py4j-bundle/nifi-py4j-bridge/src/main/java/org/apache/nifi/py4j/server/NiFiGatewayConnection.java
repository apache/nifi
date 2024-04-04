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

package org.apache.nifi.py4j.server;

import py4j.Gateway;
import py4j.GatewayConnection;
import py4j.GatewayServerListener;
import py4j.commands.Command;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

/**
 * A customized version of the GatewayConnection that is more suitable for NiFi's needs.
 *
 * With the Py4J GatewayConnection, If there's a SocketTimeoutException, py4j will kill the Connection.
 * This results in the Python side not being able to send any information back to the Java side.
 * Perhaps this makes sense for some use cases, but it does not make sense for our use case.
 *
 * With the default behavior, if we go N seconds (where N is the timeout) without communicating with the Python
 * side, the Python Process essentially becomes defunct because it can no longer communicate with the NiFi side.
 *
 * This means that if the timeout is configured for, say 30 seconds, and we go 30 seconds on a NiFi instance with no
 * Python processor, we cannot create a Python Processor. Or, if we go 30 seconds without sending a FlowFile to a
 * Processor, then we can no longer make use of the Processor.
 *
 * Because this doesn't make sense for our use case, we extend the existing GatewayConnection, and modify it in a couple
 * of ways:
 * <ul>
 *     <li>
 *         When <code>run()</code> is called, we execute super.run() continually until the connection gets poisoned
 *         (due to some Exception other than SocketTimeoutException). Or until the GatewayServer has been shutdown.
 *         This way, even when a SocketTimeoutException occurs, we continue trying to read from the socket and leave
 *         the connection open.
 *     </li>
 *     <li>
 *         Because the super class calls <code>shutdown(boolean)</code> from within <code>run()</code> for any Exception
 *         that occurs, even SocketTimeoutException, we have to engineer a way around this. Before calling <code>shutdown</code>
 *         it calls <code>quietSendFatalError(BufferedWriter, Exception)</code> with the Exception that caused it to shutdown.
 *         We use this to mark the connection as poisoned if and only if the Exception is not a SocketTimeoutException. If a
 *         SocketTimeoutException occurs, we simply ignore it. Then, in the <code>shutdown(boolean)</code> method, we do not
 *         perform the shutdown unless either the connection is poisoned or the GatewayServer has been shutdown (which indicates
 *         that communications with the Process are to be shutdown).
 *     </li>
 * </ul>
 * -
 */
public class NiFiGatewayConnection extends GatewayConnection {

    private final NiFiGatewayServer gatewayServer;
    private final ClassLoader contextClassLoader;

    public NiFiGatewayConnection(final NiFiGatewayServer gatewayServer,
                                 final Gateway gateway,
                                 final Socket socket,
                                 final String authToken,
                                 final List<Class<? extends Command>> customCommands,
                                 final List<GatewayServerListener> listeners) throws IOException {
        super(gateway, socket, authToken, customCommands, listeners);
        this.gatewayServer = gatewayServer;
        this.contextClassLoader = getClass().getClassLoader();
    }

    @Override
    public void run() {
        final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(this.contextClassLoader);

            Thread.currentThread().setName(String.format("NiFiGatewayConnection Thread for %s %s", gatewayServer.getComponentType(), gatewayServer.getComponentId()));
            super.run();

            shutdown(false);
        } finally {
            if (originalClassLoader != null) {
                Thread.currentThread().setContextClassLoader(originalClassLoader);
            }
        }
    }
}
