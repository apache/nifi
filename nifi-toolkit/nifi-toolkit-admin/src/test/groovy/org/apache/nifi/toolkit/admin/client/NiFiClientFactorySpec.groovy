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

package org.apache.nifi.toolkit.admin.client

import org.apache.nifi.properties.NiFiPropertiesLoader
import org.apache.nifi.util.NiFiProperties
import spock.lang.Specification

class NiFiClientFactorySpec extends Specification {

    def "get client for unsecure nifi"(){

        given:
        def NiFiProperties niFiProperties = Mock NiFiProperties
        def clientFactory = new NiFiClientFactory()

        when:
        def client = clientFactory.getClient(niFiProperties,"src/test/resources/notify")

        then:
        client

    }

    def "get client for secured nifi"(){

        given:

        def bootstrapConfFile = "src/test/resources/notify/conf/bootstrap.conf"
        def nifiPropertiesFile = "src/test/resources/notify/conf/nifi-secured.properties"
        def key = NiFiPropertiesLoader.extractKeyFromBootstrapFile(bootstrapConfFile)
        def NiFiProperties niFiProperties = NiFiPropertiesLoader.withKey(key).load(nifiPropertiesFile)
        def clientFactory = new NiFiClientFactory()

        when:
        def client = clientFactory.getClient(niFiProperties,"src/test/resources/notify")

        then:
        client


    }

}
