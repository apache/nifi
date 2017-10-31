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

package org.apache.nifi.processors.mqtt.common;

import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttWireMessage;

public class MqttTestClient implements IMqttAsyncClient {

	public String serverURI;
	public String clientId;

	public AtomicBoolean connected = new AtomicBoolean(false);

	public MqttCallback mqttCallback;
	public ConnectType type;

	public enum ConnectType {
		Publisher, Subscriber
	}

	public MQTTQueueMessage publishedMessage;

	public String subscribedTopic;
	public int subscribedQos;

	public MqttTestClient(String serverURI, String clientId, ConnectType type) throws MqttException {
		this.serverURI = serverURI;
		this.clientId = clientId;
		this.type = type;
	}

	@Override
	public IMqttToken connect() throws MqttSecurityException, MqttException {
		connected.set(true);
		return new IMqttToken() {

			@Override
			public void waitForCompletion(long timeout) throws MqttException {
				// TODO Auto-generated method stub

			}

			@Override
			public void waitForCompletion() throws MqttException {
				// TODO Auto-generated method stub

			}

			@Override
			public void setUserContext(Object userContext) {
				// TODO Auto-generated method stub

			}

			@Override
			public void setActionCallback(IMqttActionListener listener) {
				// TODO Auto-generated method stub

			}

			@Override
			public boolean isComplete() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public Object getUserContext() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String[] getTopics() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public boolean getSessionPresent() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public MqttWireMessage getResponse() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public int getMessageId() {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public int[] getGrantedQos() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public MqttException getException() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public IMqttAsyncClient getClient() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public IMqttActionListener getActionCallback() {
				// TODO Auto-generated method stub
				return null;
			}
		};
	}

	@Override
	public IMqttToken connect(MqttConnectOptions options) throws MqttSecurityException, MqttException {
		connected.set(true);
		return new IMqttToken() {

			@Override
			public void waitForCompletion(long timeout) throws MqttException {
				// TODO Auto-generated method stub

			}

			@Override
			public void waitForCompletion() throws MqttException {
				// TODO Auto-generated method stub

			}

			@Override
			public void setUserContext(Object userContext) {
				// TODO Auto-generated method stub

			}

			@Override
			public void setActionCallback(IMqttActionListener listener) {
				// TODO Auto-generated method stub

			}

			@Override
			public boolean isComplete() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public Object getUserContext() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String[] getTopics() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public boolean getSessionPresent() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public MqttWireMessage getResponse() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public int getMessageId() {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public int[] getGrantedQos() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public MqttException getException() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public IMqttAsyncClient getClient() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public IMqttActionListener getActionCallback() {
				// TODO Auto-generated method stub
				return null;
			}
		};
	}

	@Override
	public IMqttToken disconnect() throws MqttException {
		connected.set(false);
		return null;
	}

	@Override
	public IMqttToken disconnect(long quiesceTimeout) throws MqttException {
		connected.set(false);
		return null;
	}

	@Override
	public void disconnectForcibly() throws MqttException {
		connected.set(false);
	}

	@Override
	public void disconnectForcibly(long disconnectTimeout) throws MqttException {
		connected.set(false);
	}

	@Override
	public void disconnectForcibly(long quiesceTimeout, long disconnectTimeout) throws MqttException {
		connected.set(false);
	}

	@Override
	public IMqttToken subscribe(String topicFilter, int qos) throws MqttException {
		subscribedTopic = topicFilter;
		subscribedQos = qos;
		return null;
	}

	@Override
	public IMqttToken subscribe(String[] topicFilters, int[] qos) throws MqttException {
		throw new UnsupportedOperationException("Multiple topic filters is not supported");
	}

	@Override
	public IMqttToken unsubscribe(String topicFilter) throws MqttException {
		subscribedTopic = "";
		subscribedQos = -2;
		return null;
	}

	@Override
	public IMqttToken unsubscribe(String[] topicFilters) throws MqttException {
		throw new UnsupportedOperationException("Multiple topic filters is not supported");
	}

	@Override
	public IMqttDeliveryToken publish(String topic, byte[] payload, int qos, boolean retained)
			throws MqttException, MqttPersistenceException {
		MqttMessage message = new MqttMessage(payload);
		message.setQos(qos);
		message.setRetained(retained);
		switch (type) {
		case Publisher:
			publishedMessage = new MQTTQueueMessage(topic, message);
			break;
		case Subscriber:
			try {
				mqttCallback.messageArrived(topic, message);
			} catch (Exception e) {
				throw new MqttException(e);
			}
			break;
		}
		return null;
	}

	@Override
	public IMqttDeliveryToken publish(String topic, MqttMessage message) throws MqttException, MqttPersistenceException {
		switch (type) {
		case Publisher:
			publishedMessage = new MQTTQueueMessage(topic, message);
			break;
		case Subscriber:
			try {
				mqttCallback.messageArrived(topic, message);
			} catch (Exception e) {
				throw new MqttException(e);
			}
			break;
		}
		return null;
	}

	@Override
	public void setCallback(MqttCallback callback) {
		this.mqttCallback = callback;
	}

	@Override
	public boolean isConnected() {
		return connected.get();
	}

	@Override
	public String getClientId() {
		return clientId;
	}

	@Override
	public String getServerURI() {
		return serverURI;
	}

	@Override
	public IMqttDeliveryToken[] getPendingDeliveryTokens() {
		return new IMqttDeliveryToken[0];
	}

	@Override
	public void close() throws MqttException {

	}

	@Override
	public IMqttToken connect(Object userContext, IMqttActionListener callback) throws MqttException, MqttSecurityException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IMqttToken connect(MqttConnectOptions options, Object userContext, IMqttActionListener callback)
			throws MqttException, MqttSecurityException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IMqttToken disconnect(Object userContext, IMqttActionListener callback) throws MqttException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IMqttToken disconnect(long quiesceTimeout, Object userContext, IMqttActionListener callback) throws MqttException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IMqttDeliveryToken publish(String topic, byte[] payload, int qos, boolean retained, Object userContext,
			IMqttActionListener callback) throws MqttException, MqttPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IMqttDeliveryToken publish(String topic, MqttMessage message, Object userContext, IMqttActionListener callback)
			throws MqttException, MqttPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IMqttToken subscribe(String topicFilter, int qos, Object userContext, IMqttActionListener callback) throws MqttException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IMqttToken subscribe(String[] topicFilters, int[] qos, Object userContext, IMqttActionListener callback) throws MqttException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IMqttToken subscribe(String topicFilter, int qos, Object userContext, IMqttActionListener callback,
			IMqttMessageListener messageListener) throws MqttException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IMqttToken subscribe(String topicFilter, int qos, IMqttMessageListener messageListener) throws MqttException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IMqttToken subscribe(String[] topicFilters, int[] qos, IMqttMessageListener[] messageListeners) throws MqttException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IMqttToken subscribe(String[] topicFilters, int[] qos, Object userContext, IMqttActionListener callback,
			IMqttMessageListener[] messageListeners) throws MqttException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IMqttToken unsubscribe(String topicFilter, Object userContext, IMqttActionListener callback) throws MqttException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IMqttToken unsubscribe(String[] topicFilters, Object userContext, IMqttActionListener callback) throws MqttException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setManualAcks(boolean manualAcks) {
		// TODO Auto-generated method stub

	}

	@Override
	public void messageArrivedComplete(int messageId, int qos) throws MqttException {
		// TODO Auto-generated method stub

	}
}
