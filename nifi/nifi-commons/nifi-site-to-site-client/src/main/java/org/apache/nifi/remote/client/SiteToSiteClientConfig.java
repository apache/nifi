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
package org.apache.nifi.remote.client;

import java.io.File;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.remote.protocol.DataPacket;

public interface SiteToSiteClientConfig {

	/**
	 * Returns the configured URL for the remote NiFi instance
	 * @return
	 */
	String getUrl();

	/**
	 * Returns the communications timeout in nanoseconds
	 * @return
	 */
	long getTimeout(final TimeUnit timeUnit);

	/**
	 * Returns the amount of time that a particular node will be ignored after a
	 * communications error with that node occurs
	 * @param timeUnit
	 * @return
	 */
	long getPenalizationPeriod(TimeUnit timeUnit);

	/**
	 * Returns the SSL Context that is configured for this builder
	 * @return
	 */
	SSLContext getSslContext();
	
	/**
	 * Returns the EventReporter that is to be used by clients to report events
	 * @return
	 */
	EventReporter getEventReporter();

	/**
	 * Returns the file that is to be used for persisting the nodes of a remote cluster, if any.
	 * @return
	 */
	File getPeerPersistenceFile();

	/**
	 * Returns a boolean indicating whether or not compression will be used to transfer data
	 * to and from the remote instance
	 * @return
	 */
	boolean isUseCompression();

	/**
	 * Returns the name of the port that the client is to communicate with.
	 * @return
	 */
	String getPortName();

	/**
	 * Returns the identifier of the port that the client is to communicate with.
	 * @return
	 */
	String getPortIdentifier();
	
	/**
	 * When pulling data from a NiFi instance, the sender chooses how large a Transaction is. However,
	 * the client has the ability to request a particular batch size/duration. This returns the maximum
	 * amount of time that we will request a NiFi instance to send data to us in a Transaction.
	 * 
	 * @param timeUnit
	 * @return
	 */
	long getPreferredBatchDuration(TimeUnit timeUnit);
	
    /**
     * When pulling data from a NiFi instance, the sender chooses how large a Transaction is. However,
     * the client has the ability to request a particular batch size/duration. This returns the maximum
     * number of bytes that we will request a NiFi instance to send data to us in a Transaction.
     * 
     * @return
     */
	long getPreferredBatchSize();
	
	
	/**
     * When pulling data from a NiFi instance, the sender chooses how large a Transaction is. However,
     * the client has the ability to request a particular batch size/duration. This returns the maximum
     * number of {@link DataPacket}s that we will request a NiFi instance to send data to us in a Transaction.
     * 
     * @return
     */
	int getPreferredBatchCount();
}
