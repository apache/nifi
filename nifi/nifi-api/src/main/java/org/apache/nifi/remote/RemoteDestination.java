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

import java.util.concurrent.TimeUnit;


/**
 * A model object for referring to a remote destination (i.e., a Port) for site-to-site communications
 */
public interface RemoteDestination {
    /**
     * Returns the identifier of the remote destination
     * 
     * @return
     */
	String getIdentifier();

	/**
	 * Returns the human-readable name of the remote destination
	 * @return
	 */
	String getName();

	/**
	 * Returns the amount of time that system should pause sending to a particular node if unable to 
	 * send data to or receive data from this endpoint
	 * @param timeUnit
	 * @return
	 */
	long getYieldPeriod(TimeUnit timeUnit);
	
	/**
	 * Returns whether or not compression should be used when transferring data to or receiving
	 * data from the remote endpoint
	 * @return
	 */
	boolean isUseCompression();
}
