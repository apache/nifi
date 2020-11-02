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
package org.apache.nifi.web.security.saml;

import org.springframework.security.saml.key.KeyManager;
import org.springframework.security.saml.log.SAMLLogger;
import org.springframework.security.saml.metadata.ExtendedMetadata;
import org.springframework.security.saml.metadata.MetadataManager;
import org.springframework.security.saml.processor.SAMLProcessor;
import org.springframework.security.saml.websso.SingleLogoutProfile;
import org.springframework.security.saml.websso.WebSSOProfile;
import org.springframework.security.saml.websso.WebSSOProfileConsumer;
import org.springframework.security.saml.websso.WebSSOProfileOptions;

import java.util.Timer;

public interface SAMLConfiguration {

    String getSpEntityId();

    SAMLProcessor getProcessor();

    NiFiSAMLContextProvider getContextProvider();

    SAMLLogger getLogger();

    WebSSOProfileOptions getWebSSOProfileOptions();

    WebSSOProfile getWebSSOProfile();

    WebSSOProfile getWebSSOProfileECP();

    WebSSOProfile getWebSSOProfileHoK();

    WebSSOProfileConsumer getWebSSOProfileConsumer();

    WebSSOProfileConsumer getWebSSOProfileHoKConsumer();

    SingleLogoutProfile getSingleLogoutProfile();

    ExtendedMetadata getExtendedMetadata();

    MetadataManager getMetadataManager();

    KeyManager getKeyManager();

    Timer getBackgroundTaskTimer();

    long getAuthExpiration();

    String getIdentityAttributeName();

    String getGroupAttributeName();

    boolean isRequestSigningEnabled();

    boolean isWantAssertionsSigned();

}
