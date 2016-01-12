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
package org.apache.nifi.bootstrap.notification;

import org.apache.nifi.components.ConfigurableComponent;

/**
 * <p>
 * A NotificationService is simple mechanism that the Bootstrap can use to notify
 * interested parties when some event takes place, such as NiFi being started, stopped,
 * or restarted because the process died.
 * </p>
 *
 * <p>
 * <b>Note:</b> This feature was introduced in version 0.3.0 of NiFi and is likely to undergo
 * significant refactorings. As such, at this time it is NOT considered a public API and may well
 * change from version to version until the API has stabilized. At that point, it will become a public
 * API.
 * </p>
 *
 * @since 0.3.0
 */
public interface NotificationService extends ConfigurableComponent {

    /**
     * Provides the NotificatoinService with access to objects that may be of use
     * throughout the life of the service
     *
     * @param context of initialization
     */
    void initialize(NotificationInitializationContext context);

    /**
     * Notifies the configured recipients of some event
     *
     * @param context the context that is relevant for this notification
     * @param subject the subject of the message
     * @param message the message to be provided to recipients
     */
    void notify(NotificationContext context, String subject, String message) throws NotificationFailedException;

}
