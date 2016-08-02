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

package org.apache.nifi.bootstrap

import org.apache.nifi.bootstrap.notification.NotificationType
import org.apache.nifi.registry.VariableRegistry
import spock.lang.Specification
import java.nio.file.Paths

class NotificationServiceManagerSpec extends Specification{

    def setupSpec(){
    }

    def "should acess variable registry to replace EL values"(){

        given:
            def mockRegistry = Mock(VariableRegistry.class)
            def notificationServiceManager = new NotificationServiceManager(mockRegistry);
            def file = Paths.get("src/test/resources/notification-services.xml").toFile()
            notificationServiceManager.loadNotificationServices(file)
            //testing with stopped becasue it will block until method is completed
            notificationServiceManager.registerNotificationService(NotificationType.NIFI_STOPPED,"custom-notification")

        when:
            notificationServiceManager.notify(NotificationType.NIFI_STOPPED,"NiFi Stopped","NiFi Stopped")

        then:
            6 * mockRegistry.getVariables() >> ["test.server":"smtp://fakeserver.com","test.username":"user","test.password":"pass"]


    }


}
