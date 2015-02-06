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
package org.apache.nifi.remote;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.nifi.remote.exception.HandshakeException;

public class RemoteResourceInitiator {
	public static final int RESOURCE_OK = 20;
	public static final int DIFFERENT_RESOURCE_VERSION = 21;
	public static final int ABORT = 255;

	
	public static VersionedRemoteResource initiateResourceNegotiation(final VersionedRemoteResource resource, final DataInputStream dis, final DataOutputStream dos) throws IOException, HandshakeException {
        // Write the classname of the RemoteStreamCodec, followed by its version
    	dos.writeUTF(resource.getResourceName());
    	final VersionNegotiator negotiator = resource.getVersionNegotiator();
    	dos.writeInt(negotiator.getVersion());
    	dos.flush();
        
        // wait for response from server.
        final int statusCode = dis.read();
        switch (statusCode) {
            case RESOURCE_OK:	// server accepted our proposal of codec name/version
                return resource;
            case DIFFERENT_RESOURCE_VERSION:	// server accepted our proposal of codec name but not the version
                // Get server's preferred version
            	final int newVersion = dis.readInt();
                
                // Determine our new preferred version that is no greater than the server's preferred version.
                final Integer newPreference = negotiator.getPreferredVersion(newVersion);
                // If we could not agree with server on a version, fail now.
                if ( newPreference == null ) {
                    throw new HandshakeException("Could not agree on version for " + resource);
                }
                
                negotiator.setVersion(newPreference);
                
                // Attempt negotiation of resource based on our new preferred version.
                return initiateResourceNegotiation(resource, dis, dos);
            case ABORT:
            	throw new HandshakeException("Remote destination aborted connection with message: " + dis.readUTF());
            default:
                return null;	// Unable to negotiate codec
        }
	}
}
