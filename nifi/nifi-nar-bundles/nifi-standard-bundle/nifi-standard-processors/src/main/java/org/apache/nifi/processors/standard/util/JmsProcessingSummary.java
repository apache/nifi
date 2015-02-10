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
package org.apache.nifi.processors.standard.util;

import javax.jms.Message;

import org.apache.nifi.flowfile.FlowFile;


/**
 * 	Data structure which allows to collect processing summary data. 
 * 
 */
public class JmsProcessingSummary {
	
	private int			messagesReceived;
	private long		bytesReceived;
	private Message 	lastMessageReceived;
	private int	 		flowFilesCreated;
	private FlowFile	lastFlowFile;			// helps testing

	public JmsProcessingSummary() {
		super();
		this.messagesReceived 	= 0;
		this.bytesReceived 		= 0;
		this.lastMessageReceived = null;
		this.flowFilesCreated 	= 0;
		this.lastFlowFile 		= null;
	}
	
	public JmsProcessingSummary(long bytesReceived, Message lastMessageReceived, FlowFile lastFlowFile) {
		super();
		this.messagesReceived 	= 1;
		this.bytesReceived 		= bytesReceived;
		this.lastMessageReceived = lastMessageReceived;
		this.flowFilesCreated 	= 1;
		this.lastFlowFile 		= lastFlowFile;
	}
	
	public void add(JmsProcessingSummary jmsProcessingSummary) {
		this.messagesReceived 	+= jmsProcessingSummary.messagesReceived;
		this.bytesReceived 		+= jmsProcessingSummary.bytesReceived;
		this.lastMessageReceived = jmsProcessingSummary.lastMessageReceived;
		this.flowFilesCreated 	+= jmsProcessingSummary.flowFilesCreated;
		this.lastFlowFile 		=  jmsProcessingSummary.lastFlowFile;
	}

	public int getMessagesReceived() {
		return messagesReceived;
	}

	public long getBytesReceived() {
		return bytesReceived;
	}

	public Message getLastMessageReceived() {
		return lastMessageReceived;
	}

	public int getFlowFilesCreated() {
		return flowFilesCreated;
	}

	public FlowFile getLastFlowFile() {
		return lastFlowFile;
	}
	
}

