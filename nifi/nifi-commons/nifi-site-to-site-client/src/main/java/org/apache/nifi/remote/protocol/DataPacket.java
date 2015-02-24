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
package org.apache.nifi.remote.protocol;

import java.io.InputStream;
import java.util.Map;


/**
 * Represents a piece of data that is to be sent to or that was received from a NiFi instance.
 */
public interface DataPacket {

    /**
     * The key-value attributes that are to be associated with the data
     * @return
     */
	Map<String, String> getAttributes();
	
	/**
	 * An InputStream from which the content can be read
	 * @return
	 */
	InputStream getData();

	/**
	 * The length of the InputStream.
	 * @return
	 */
	long getSize();
}
