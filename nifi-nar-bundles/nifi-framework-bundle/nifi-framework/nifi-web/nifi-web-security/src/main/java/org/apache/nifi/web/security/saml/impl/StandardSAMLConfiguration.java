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
package org.apache.nifi.web.security.saml.impl;

import org.apache.nifi.web.security.saml.NiFiSAMLContextProvider;
import org.apache.nifi.web.security.saml.SAMLConfiguration;
import org.springframework.security.saml.key.KeyManager;
import org.springframework.security.saml.log.SAMLLogger;
import org.springframework.security.saml.metadata.ExtendedMetadata;
import org.springframework.security.saml.metadata.MetadataManager;
import org.springframework.security.saml.processor.SAMLProcessor;
import org.springframework.security.saml.websso.SingleLogoutProfile;
import org.springframework.security.saml.websso.WebSSOProfile;
import org.springframework.security.saml.websso.WebSSOProfileConsumer;
import org.springframework.security.saml.websso.WebSSOProfileOptions;

import java.util.Objects;
import java.util.Timer;

public class StandardSAMLConfiguration implements SAMLConfiguration {

    private final String spEntityId;

    private final SAMLProcessor processor;
    private final NiFiSAMLContextProvider contextProvider;
    private final SAMLLogger logger;

    private final WebSSOProfileOptions webSSOProfileOptions;

    private final WebSSOProfile webSSOProfile;
    private final WebSSOProfile webSSOProfileECP;
    private final WebSSOProfile webSSOProfileHoK;

    private final WebSSOProfileConsumer webSSOProfileConsumer;
    private final WebSSOProfileConsumer webSSOProfileHoKConsumer;

    private final SingleLogoutProfile singleLogoutProfile;

    private final ExtendedMetadata extendedMetadata;
    private final MetadataManager metadataManager;

    private final KeyManager keyManager;
    private final Timer backgroundTaskTimer;

    private final long authExpiration;
    private final String identityAttributeName;
    private final String groupAttributeName;

    private final boolean requestSigningEnabled;
    private final boolean wantAssertionsSigned;

    private StandardSAMLConfiguration(final Builder builder) {
        this.spEntityId = Objects.requireNonNull(builder.spEntityId);
        this.processor = Objects.requireNonNull(builder.processor);
        this.contextProvider = Objects.requireNonNull(builder.contextProvider);
        this.logger = Objects.requireNonNull(builder.logger);
        this.webSSOProfileOptions = Objects.requireNonNull(builder.webSSOProfileOptions);
        this.webSSOProfile = Objects.requireNonNull(builder.webSSOProfile);
        this.webSSOProfileECP = Objects.requireNonNull(builder.webSSOProfileECP);
        this.webSSOProfileHoK = Objects.requireNonNull(builder.webSSOProfileHoK);
        this.webSSOProfileConsumer = Objects.requireNonNull(builder.webSSOProfileConsumer);
        this.webSSOProfileHoKConsumer = Objects.requireNonNull(builder.webSSOProfileHoKConsumer);
        this.singleLogoutProfile = Objects.requireNonNull(builder.singleLogoutProfile);
        this.extendedMetadata = Objects.requireNonNull(builder.extendedMetadata);
        this.metadataManager = Objects.requireNonNull(builder.metadataManager);
        this.keyManager = Objects.requireNonNull(builder.keyManager);
        this.backgroundTaskTimer = Objects.requireNonNull(builder.backgroundTaskTimer);
        this.authExpiration = builder.authExpiration;
        this.identityAttributeName = builder.identityAttributeName;
        this.groupAttributeName = builder.groupAttributeName;
        this.requestSigningEnabled = builder.requestSigningEnabled;
        this.wantAssertionsSigned = builder.wantAssertionsSigned;
    }

    @Override
    public String getSpEntityId() {
        return spEntityId;
    }

    @Override
    public SAMLProcessor getProcessor() {
        return processor;
    }

    @Override
    public NiFiSAMLContextProvider getContextProvider() {
        return contextProvider;
    }

    @Override
    public SAMLLogger getLogger() {
        return logger;
    }

    @Override
    public WebSSOProfileOptions getWebSSOProfileOptions() {
        return webSSOProfileOptions;
    }

    @Override
    public WebSSOProfile getWebSSOProfile() {
        return webSSOProfile;
    }

    @Override
    public WebSSOProfile getWebSSOProfileECP() {
        return webSSOProfileECP;
    }

    @Override
    public WebSSOProfile getWebSSOProfileHoK() {
        return webSSOProfileHoK;
    }

    @Override
    public WebSSOProfileConsumer getWebSSOProfileConsumer() {
        return webSSOProfileConsumer;
    }

    @Override
    public WebSSOProfileConsumer getWebSSOProfileHoKConsumer() {
        return webSSOProfileHoKConsumer;
    }

    @Override
    public SingleLogoutProfile getSingleLogoutProfile() {
        return singleLogoutProfile;
    }

    @Override
    public ExtendedMetadata getExtendedMetadata() {
        return extendedMetadata;
    }

    @Override
    public MetadataManager getMetadataManager() {
        return metadataManager;
    }

    @Override
    public KeyManager getKeyManager() {
        return keyManager;
    }

    @Override
    public Timer getBackgroundTaskTimer() {
        return backgroundTaskTimer;
    }

    @Override
    public long getAuthExpiration() {
        return authExpiration;
    }

    @Override
    public String getIdentityAttributeName() {
        return identityAttributeName;
    }

    @Override
    public String getGroupAttributeName() {
        return groupAttributeName;
    }

    @Override
    public boolean isRequestSigningEnabled() {
        return requestSigningEnabled;
    }

    @Override
    public boolean isWantAssertionsSigned() {
        return wantAssertionsSigned;
    }

    /**
     * Builder for SAMLConfiguration.
     */
    public static class Builder {

        private String spEntityId;

        private SAMLProcessor processor;
        private NiFiSAMLContextProvider contextProvider;
        private SAMLLogger logger;

        private WebSSOProfileOptions webSSOProfileOptions;

        private WebSSOProfile webSSOProfile;
        private WebSSOProfile webSSOProfileECP;
        private WebSSOProfile webSSOProfileHoK;

        private WebSSOProfileConsumer webSSOProfileConsumer;
        private WebSSOProfileConsumer webSSOProfileHoKConsumer;

        private SingleLogoutProfile singleLogoutProfile;

        private ExtendedMetadata extendedMetadata;
        private MetadataManager metadataManager;

        private KeyManager keyManager;
        private Timer backgroundTaskTimer;

        private long authExpiration;
        private String groupAttributeName;
        private String identityAttributeName;

        private boolean requestSigningEnabled;
        private boolean wantAssertionsSigned;

        public Builder spEntityId(String spEntityId) {
            this.spEntityId = spEntityId;
            return this;
        }

        public Builder processor(SAMLProcessor processor) {
            this.processor = processor;
            return this;
        }

        public Builder contextProvider(NiFiSAMLContextProvider contextProvider) {
            this.contextProvider = contextProvider;
            return this;
        }

        public Builder logger(SAMLLogger logger) {
            this.logger = logger;
            return this;
        }

        public Builder webSSOProfileOptions(WebSSOProfileOptions webSSOProfileOptions) {
            this.webSSOProfileOptions = webSSOProfileOptions;
            return this;
        }

        public Builder webSSOProfile(WebSSOProfile webSSOProfile) {
            this.webSSOProfile = webSSOProfile;
            return this;
        }

        public Builder webSSOProfileECP(WebSSOProfile webSSOProfileECP) {
            this.webSSOProfileECP = webSSOProfileECP;
            return this;
        }

        public Builder webSSOProfileHoK(WebSSOProfile webSSOProfileHoK) {
            this.webSSOProfileHoK = webSSOProfileHoK;
            return this;
        }

        public Builder webSSOProfileConsumer(WebSSOProfileConsumer webSSOProfileConsumer) {
            this.webSSOProfileConsumer = webSSOProfileConsumer;
            return this;
        }

        public Builder webSSOProfileHoKConsumer(WebSSOProfileConsumer webSSOProfileHoKConsumer) {
            this.webSSOProfileHoKConsumer = webSSOProfileHoKConsumer;
            return this;
        }

        public Builder singleLogoutProfile(SingleLogoutProfile singleLogoutProfile) {
            this.singleLogoutProfile = singleLogoutProfile;
            return this;
        }

        public Builder extendedMetadata(ExtendedMetadata extendedMetadata) {
            this.extendedMetadata = extendedMetadata;
            return this;
        }

        public Builder metadataManager(MetadataManager metadataManager) {
            this.metadataManager = metadataManager;
            return this;
        }

        public Builder keyManager(KeyManager keyManager) {
            this.keyManager = keyManager;
            return this;
        }

        public Builder backgroundTaskTimer(Timer backgroundTaskTimer) {
            this.backgroundTaskTimer = backgroundTaskTimer;
            return this;
        }

        public Builder authExpiration(long authExpiration) {
            this.authExpiration = authExpiration;
            return this;
        }

        public Builder identityAttributeName(String identityAttributeName) {
            this.identityAttributeName = identityAttributeName;
            return this;
        }

        public Builder groupAttributeName(String groupAttributeName) {
            this.groupAttributeName = groupAttributeName;
            return this;
        }

        public Builder requestSigningEnabled(boolean requestSigningEnabled) {
            this.requestSigningEnabled = requestSigningEnabled;
            return this;
        }

        public Builder wantAssertionsSigned(boolean wantAssertionsSigned) {
            this.wantAssertionsSigned = wantAssertionsSigned;
            return this;
        }

        public SAMLConfiguration build() {
            return new StandardSAMLConfiguration(this);
        }
    }
}
