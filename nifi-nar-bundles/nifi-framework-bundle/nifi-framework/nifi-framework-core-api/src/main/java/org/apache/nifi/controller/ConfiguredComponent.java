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
package org.apache.nifi.controller;

import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.ComponentAuthorizable;
import org.apache.nifi.authorization.resource.RestrictedComponentsAuthorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.registry.ComponentVariableRegistry;

import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ConfiguredComponent extends ComponentAuthorizable {

    @Override
    public String getIdentifier();

    public String getName();

    public void setName(String name);

    public String getAnnotationData();

    public void setAnnotationData(String data);

    public void setProperties(Map<String, String> properties);

    public Map<PropertyDescriptor, String> getProperties();

    public String getProperty(final PropertyDescriptor property);

    boolean isValid();

    void reload(Set<URL> additionalUrls) throws Exception;

    void refreshProperties();

    Set<URL> getAdditionalClasspathResources(List<PropertyDescriptor> propertyDescriptors);

    BundleCoordinate getBundleCoordinate();

    ConfigurableComponent getComponent();

    ComponentLog getLogger();

    boolean isExtensionMissing();

    void setExtensionMissing(boolean extensionMissing);

    void verifyCanUpdateBundle(BundleCoordinate bundleCoordinate) throws IllegalStateException;

    /**
     * @return the any validation errors for this connectable
     */
    Collection<ValidationResult> getValidationErrors();

    /**
     * @return the type of the component. I.e., the class name of the implementation
     */
    String getComponentType();

    /**
     * @return the Canonical Class Name of the component
     */
    String getCanonicalClassName();

    /**
     * @return whether or not the underlying implementation is restricted
     */
    boolean isRestricted();

    /**
     * @return whether or not the underlying implementation is deprecated
     */
    boolean isDeprecated();

    /**
     * @return the variable registry for this component
     */
    ComponentVariableRegistry getVariableRegistry();

    @Override
    default AuthorizationResult checkAuthorization(Authorizer authorizer, RequestAction action, NiFiUser user, Map<String, String> resourceContext) {
        // if this is a modification request and the reporting task is restricted ensure the user has elevated privileges. if this
        // is not a modification request, we just want to use the normal rules
        if (RequestAction.WRITE.equals(action) && isRestricted()) {
            final RestrictedComponentsAuthorizable restrictedComponentsAuthorizable = new RestrictedComponentsAuthorizable();
            final AuthorizationResult result = restrictedComponentsAuthorizable.checkAuthorization(authorizer, RequestAction.WRITE, user, resourceContext);
            if (Result.Denied.equals(result.getResult())) {
                return result;
            }
        }

        // defer to the base authorization check
        return ComponentAuthorizable.super.checkAuthorization(authorizer, action, user, resourceContext);
    }

    @Override
    default void authorize(Authorizer authorizer, RequestAction action, NiFiUser user, Map<String, String> resourceContext) throws AccessDeniedException {
        // if this is a modification request and the reporting task is restricted ensure the user has elevated privileges. if this
        // is not a modification request, we just want to use the normal rules
        if (RequestAction.WRITE.equals(action) && isRestricted()) {
            final RestrictedComponentsAuthorizable restrictedComponentsAuthorizable = new RestrictedComponentsAuthorizable();
            restrictedComponentsAuthorizable.authorize(authorizer, RequestAction.WRITE, user, resourceContext);
        }

        // defer to the base authorization check
        ComponentAuthorizable.super.authorize(authorizer, action, user, resourceContext);
    }
}
