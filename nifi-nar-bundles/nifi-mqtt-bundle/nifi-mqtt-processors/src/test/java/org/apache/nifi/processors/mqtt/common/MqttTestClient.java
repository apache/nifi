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

import org.eclipse.paho.client.mqttv3.*;

import java.util.concurrent.atomic.AtomicBoolean;

public class MqttTestClient implements IMqttClient {

    public String serverURI;
    public String clientId;

    public AtomicBoolean connected = new AtomicBoolean(false);

    public MqttCallback mqttCallback;
    public ConnectType type;
    public enum ConnectType {Publisher, Subscriber}

    public MQTTQueueMessage publishedMessage;

    public String subscribedTopic;
    public int subscribedQos;


    public MqttTestClient(String serverURI, String clientId, ConnectType type) throws MqttException {
        this.serverURI = serverURI;
        this.clientId = clientId;
        this.type = type;
    }

    @Override
    public void connect() throws MqttSecurityException, MqttException {
        connected.set(true);
    }

    @Override
    public void connect(MqttConnectOptions options) throws MqttSecurityException, MqttException {
        connected.set(true);
    }

    @Override
    public IMqttToken connectWithResult(MqttConnectOptions options) throws MqttSecurityException, MqttException {
        return null;
    }

    @Override
    public void disconnect() throws MqttException {
        connected.set(false);
    }

    @Override
    public void disconnect(long quiesceTimeout) throws MqttException {
        connected.set(false);
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
    public void subscribe(String topicFilter) throws MqttException, MqttSecurityException {
        subscribedTopic = topicFilter;
        subscribedQos = -1;
    }

    @Override
    public void subscribe(String[] topicFilters) throws MqttException {
        throw new UnsupportedOperationException("Multiple topic filters is not supported");
    }

    @Override
    public void subscribe(String topicFilter, int qos) throws MqttException {
        subscribedTopic = topicFilter;
        subscribedQos = qos;
    }

    @Override
    public void subscribe(String[] topicFilters, int[] qos) throws MqttException {
        throw new UnsupportedOperationException("Multiple topic filters is not supported");
    }


    @Override
    public void subscribe(String s, IMqttMessageListener iMqttMessageListener) throws MqttException, MqttSecurityException {
        throw new UnsupportedOperationException("This function isn't supported");
    }


    @Override
    public void subscribe(String[] strings, IMqttMessageListener[] iMqttMessageListeners) throws MqttException {
        throw new UnsupportedOperationException("This function isn't supported");
    }

    @Override
    public void subscribe(String s, int i, IMqttMessageListener iMqttMessageListener) throws MqttException {
        throw new UnsupportedOperationException("This function isn't supported");
    }

    @Override
    public void subscribe(String[] strings, int[] ints, IMqttMessageListener[] iMqttMessageListeners) throws MqttException {
        throw new UnsupportedOperationException("This function isn't supported");
    }

    @Override
    public void setManualAcks(boolean b) {
        throw new UnsupportedOperationException("This function isn't supported");
    }

    @Override
    public void messageArrivedComplete(int i, int i1) throws MqttException {

    }

    @Override
    public void unsubscribe(String topicFilter) throws MqttException {
        subscribedTopic = "";
        subscribedQos = -2;
    }

    @Override
    public void unsubscribe(String[] topicFilters) throws MqttException {
        throw new UnsupportedOperationException("Multiple topic filters is not supported");
    }

    @Override
    public void publish(String topic, byte[] payload, int qos, boolean retained) throws MqttException, MqttPersistenceException {
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
    }

    @Override
    public void publish(String topic, MqttMessage message) throws MqttException, MqttPersistenceException {
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
    }

    @Override
    public void setCallback(MqttCallback callback) {
        this.mqttCallback = callback;
    }

    @Override
    public MqttTopic getTopic(String topic) {
        return null;
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

    /**
     * Subscribe to a topic, which may include wildcards using a QoS of 1.
     *
     * @param topicFilter the topic to subscribe to, which can include wildcards.
     * @return token used to track the subscribe after it has completed.
     * @throws MqttException if there was an error registering the subscription.
     * @see #subscribeWithResponse(String[], int[])
     */
    @Override
    public IMqttToken subscribeWithResponse(String topicFilter) throws MqttException {
        return null;
    }

    /**
     * Subscribe to a topic, which may include wildcards using a QoS of 1.
     *
     * @param topicFilter     the topic to subscribe to, which can include wildcards.
     * @param messageListener a callback to handle incoming messages
     * @return token used to track the subscribe after it has completed.
     * @throws MqttException if there was an error registering the subscription.
     * @see #subscribeWithResponse(String[], int[])
     */
    @Override
    public IMqttToken subscribeWithResponse(String topicFilter, IMqttMessageListener messageListener) throws MqttException {
        return null;
    }

    /**
     * Subscribe to a topic, which may include wildcards.
     *
     * @param topicFilter the topic to subscribe to, which can include wildcards.
     * @param qos         the maximum quality of service at which to subscribe. Messages
     *                    published at a lower quality of service will be received at the published
     *                    QoS.  Messages published at a higher quality of service will be received using
     *                    the QoS specified on the subscribe.
     * @return token used to track the subscribe after it has completed.
     * @throws MqttException if there was an error registering the subscription.
     * @see #subscribeWithResponse(String[], int[])
     */
    @Override
    public IMqttToken subscribeWithResponse(String topicFilter, int qos) throws MqttException {
        return null;
    }

    /**
     * Subscribe to a topic, which may include wildcards.
     *
     * @param topicFilter     the topic to subscribe to, which can include wildcards.
     * @param qos             the maximum quality of service at which to subscribe. Messages
     *                        published at a lower quality of service will be received at the published
     *                        QoS.  Messages published at a higher quality of service will be received using
     *                        the QoS specified on the subscribe.
     * @param messageListener a callback to handle incoming messages
     * @return token used to track the subscribe after it has completed.
     * @throws MqttException if there was an error registering the subscription.
     * @see #subscribeWithResponse(String[], int[])
     */
    @Override
    public IMqttToken subscribeWithResponse(String topicFilter, int qos, IMqttMessageListener messageListener) throws MqttException {
        return null;
    }

    /**
     * Subscribes to a one or more topics, which may include wildcards using a QoS of 1.
     *
     * @param topicFilters the topic to subscribe to, which can include wildcards.
     * @return token used to track the subscribe after it has completed.
     * @throws MqttException if there was an error registering the subscription.
     * @see #subscribeWithResponse(String[], int[])
     */
    @Override
    public IMqttToken subscribeWithResponse(String[] topicFilters) throws MqttException {
        return null;
    }

    /**
     * Subscribes to a one or more topics, which may include wildcards using a QoS of 1.
     *
     * @param topicFilters     the topic to subscribe to, which can include wildcards.
     * @param messageListeners one or more callbacks to handle incoming messages
     * @return token used to track the subscribe after it has completed.
     * @throws MqttException if there was an error registering the subscription.
     * @see #subscribeWithResponse(String[], int[])
     */
    @Override
    public IMqttToken subscribeWithResponse(String[] topicFilters, IMqttMessageListener[] messageListeners) throws MqttException {
        return null;
    }

    /**
     * Subscribes to multiple topics, each of which may include wildcards.
     * <p>The {@link #setCallback(MqttCallback)} method
     * should be called before this method, otherwise any received messages
     * will be discarded.
     * </p>
     * <p>
     * If (@link MqttConnectOptions#setCleanSession(boolean)} was set to true
     * when when connecting to the server then the subscription remains in place
     * until either:</p>
     * <ul>
     * <li>The client disconnects</li>
     * <li>An unsubscribe method is called to un-subscribe the topic</li>
     * </ul>
     * <p>
     * If (@link MqttConnectOptions#setCleanSession(boolean)} was set to false
     * when when connecting to the server then the subscription remains in place
     * until either:</p>
     * <ul>
     * <li>An unsubscribe method is called to unsubscribe the topic</li>
     * <li>The client connects with cleanSession set to true</li>
     * </ul>
     * <p>
     * With cleanSession set to false the MQTT server will store messages on
     * behalf of the client when the client is not connected. The next time the
     * client connects with the <b>same client ID</b> the server will
     * deliver the stored messages to the client.
     * </p>
     * <p>
     * <p>The "topic filter" string used when subscribing
     * may contain special characters, which allow you to subscribe to multiple topics
     * at once.</p>
     * <p>The topic level separator is used to introduce structure into the topic, and
     * can therefore be specified within the topic for that purpose.  The multi-level
     * wildcard and single-level wildcard can be used for subscriptions, but they
     * cannot be used within a topic by the publisher of a message.
     * <dl>
     * <dt>Topic level separator</dt>
     * <dd>The forward slash (/) is used to separate each level within
     * a topic tree and provide a hierarchical structure to the topic space. The
     * use of the topic level separator is significant when the two wildcard characters
     * are encountered in topics specified by subscribers.</dd>
     * <p>
     * <dt>Multi-level wildcard</dt>
     * <dd><p>The number sign (#) is a wildcard character that matches
     * any number of levels within a topic. For example, if you subscribe to
     * <span><span class="filepath">finance/stock/ibm/#</span></span>, you receive
     * messages on these topics:</p>
     * <ul>
     * <li><pre>finance/stock/ibm</pre></li>
     * <li><pre>finance/stock/ibm/closingprice</pre></li>
     * <li><pre>finance/stock/ibm/currentprice</pre></li>
     * </ul>
     * <p>The multi-level wildcard
     * can represent zero or more levels. Therefore, <em>finance/#</em> can also match
     * the singular <em>finance</em>, where <em>#</em> represents zero levels. The topic
     * level separator is meaningless in this context, because there are no levels
     * to separate.</p>
     * <p>
     * <p>The <span>multi-level</span> wildcard can
     * be specified only on its own or next to the topic level separator character.
     * Therefore, <em>#</em> and <em>finance/#</em> are both valid, but <em>finance#</em> is
     * not valid. <span>The multi-level wildcard must be the last character
     * used within the topic tree. For example, <em>finance/#</em> is valid but
     * <em>finance/#/closingprice</em> is 	not valid.</span></p></dd>
     * <p>
     * <dt>Single-level wildcard</dt>
     * <dd><p>The plus sign (+) is a wildcard character that matches only one topic
     * level. For example, <em>finance/stock/+</em> matches
     * <em>finance/stock/ibm</em> and <em>finance/stock/xyz</em>,
     * but not <em>finance/stock/ibm/closingprice</em>. Also, because the single-level
     * wildcard matches only a single level, <em>finance/+</em> does not match <em>finance</em>.</p>
     * <p>
     * <p>Use
     * the single-level wildcard at any level in the topic tree, and in conjunction
     * with the multilevel wildcard. Specify the single-level wildcard next to the
     * topic level separator, except when it is specified on its own. Therefore,
     * <em>+</em> and <em>finance/+</em> are both valid, but <em>finance+</em> is
     * not valid. <span>The single-level wildcard can be used at the end of the
     * topic tree or within the topic tree.
     * For example, <em>finance/+</em> and <em>finance/+/ibm</em> are both valid.</span></p>
     * </dd>
     * </dl>
     * <p>
     * <p>This is a blocking method that returns once subscribe completes</p>
     *
     * @param topicFilters one or more topics to subscribe to, which can include wildcards.
     * @param qos          the maximum quality of service to subscribe each topic at.Messages
     *                     published at a lower quality of service will be received at the published
     *                     QoS.  Messages published at a higher quality of service will be received using
     *                     the QoS specified on the subscribe.
     * @return token used to track the subscribe after it has completed.
     * @throws MqttException            if there was an error registering the subscription.
     * @throws IllegalArgumentException if the two supplied arrays are not the same size.
     */
    @Override
    public IMqttToken subscribeWithResponse(String[] topicFilters, int[] qos) throws MqttException {
        return null;
    }

    /**
     * Subscribes to multiple topics, each of which may include wildcards.
     * <p>The {@link #setCallback(MqttCallback)} method
     * should be called before this method, otherwise any received messages
     * will be discarded.
     * </p>
     * <p>
     * If (@link MqttConnectOptions#setCleanSession(boolean)} was set to true
     * when when connecting to the server then the subscription remains in place
     * until either:</p>
     * <ul>
     * <li>The client disconnects</li>
     * <li>An unsubscribe method is called to un-subscribe the topic</li>
     * </ul>
     * <p>
     * <p>
     * If (@link MqttConnectOptions#setCleanSession(boolean)} was set to false
     * when when connecting to the server then the subscription remains in place
     * until either:</p>
     * <ul>
     * <li>An unsubscribe method is called to unsubscribe the topic</li>
     * <li>The client connects with cleanSession set to true</li>
     * </ul>
     * <p>
     * With cleanSession set to false the MQTT server will store messages on
     * behalf of the client when the client is not connected. The next time the
     * client connects with the <b>same client ID</b> the server will
     * deliver the stored messages to the client.
     * </p>
     * <p>
     * <p>The "topic filter" string used when subscribing
     * may contain special characters, which allow you to subscribe to multiple topics
     * at once.</p>
     * <p>The topic level separator is used to introduce structure into the topic, and
     * can therefore be specified within the topic for that purpose.  The multi-level
     * wildcard and single-level wildcard can be used for subscriptions, but they
     * cannot be used within a topic by the publisher of a message.
     * <dl>
     * <dt>Topic level separator</dt>
     * <dd>The forward slash (/) is used to separate each level within
     * a topic tree and provide a hierarchical structure to the topic space. The
     * use of the topic level separator is significant when the two wildcard characters
     * are encountered in topics specified by subscribers.</dd>
     * <p>
     * <dt>Multi-level wildcard</dt>
     * <dd><p>The number sign (#) is a wildcard character that matches
     * any number of levels within a topic. For example, if you subscribe to
     * <span><span class="filepath">finance/stock/ibm/#</span></span>, you receive
     * messages on these topics:</p>
     * <ul>
     * <li><pre>finance/stock/ibm</pre></li>
     * <li><pre>finance/stock/ibm/closingprice</pre></li>
     * <li><pre>finance/stock/ibm/currentprice</pre></li>
     * </ul>
     * <p>The multi-level wildcard
     * can represent zero or more levels. Therefore, <em>finance/#</em> can also match
     * the singular <em>finance</em>, where <em>#</em> represents zero levels. The topic
     * level separator is meaningless in this context, because there are no levels
     * to separate.</p>
     * <p>
     * <p>The <span>multi-level</span> wildcard can
     * be specified only on its own or next to the topic level separator character.
     * Therefore, <em>#</em> and <em>finance/#</em> are both valid, but <em>finance#</em> is
     * not valid. <span>The multi-level wildcard must be the last character
     * used within the topic tree. For example, <em>finance/#</em> is valid but
     * <em>finance/#/closingprice</em> is 	not valid.</span></p></dd>
     * <p>
     * <dt>Single-level wildcard</dt>
     * <dd><p>The plus sign (+) is a wildcard character that matches only one topic
     * level. For example, <em>finance/stock/+</em> matches
     * <em>finance/stock/ibm</em> and <em>finance/stock/xyz</em>,
     * but not <em>finance/stock/ibm/closingprice</em>. Also, because the single-level
     * wildcard matches only a single level, <em>finance/+</em> does not match <em>finance</em>.</p>
     * <p>
     * <p>Use
     * the single-level wildcard at any level in the topic tree, and in conjunction
     * with the multilevel wildcard. Specify the single-level wildcard next to the
     * topic level separator, except when it is specified on its own. Therefore,
     * <em>+</em> and <em>finance/+</em> are both valid, but <em>finance+</em> is
     * not valid. <span>The single-level wildcard can be used at the end of the
     * topic tree or within the topic tree.
     * For example, <em>finance/+</em> and <em>finance/+/ibm</em> are both valid.</span></p>
     * </dd>
     * </dl>
     * <p>
     * <p>
     * <p>This is a blocking method that returns once subscribe completes</p>
     *
     * @param topicFilters     one or more topics to subscribe to, which can include wildcards.
     * @param qos              the maximum quality of service to subscribe each topic at.Messages
     *                         published at a lower quality of service will be received at the published
     *                         QoS.  Messages published at a higher quality of service will be received using
     *                         the QoS specified on the subscribe.
     * @param messageListeners one or more callbacks to handle incoming messages
     * @return token used to track the subscribe after it has completed.
     * @throws MqttException            if there was an error registering the subscription.
     * @throws IllegalArgumentException if the two supplied arrays are not the same size.
     */
    @Override
    public IMqttToken subscribeWithResponse(String[] topicFilters, int[] qos, IMqttMessageListener[] messageListeners) throws MqttException {
        return null;
    }
}
