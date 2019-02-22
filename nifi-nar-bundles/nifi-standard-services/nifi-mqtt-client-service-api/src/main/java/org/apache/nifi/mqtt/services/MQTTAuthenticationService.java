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
package org.apache.nifi.mqtt.services;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.exception.ProcessException;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

/**
 * Definition for MQTT Client Authentication controller service.
 */
@Tags({"mqtt", "connection", "client"})
@CapabilityDescription("Provides MQTT Connection according to corresponding authentication requirements.")
public interface MQTTAuthenticationService extends ControllerService {

    /**
     * Method to set/override the MQTT Connection Options
     * @param connOpts MQTT connection options initialized in the MQTT processors
     * @throws ProcessException exception when setting the configuration for the MQTT connection
     */
    void setMqttConnectOptions(MqttConnectOptions connOpts) throws ProcessException;

    /**
     * Method to refresh credentials if required
     * @param connOpts MQTT connection options initialized in the MQTT processors
     * @throws ProcessException exception when setting the configuration for the MQTT connection
     */
    void refreshConnectOptions(MqttConnectOptions connOpts) throws ProcessException;

}
