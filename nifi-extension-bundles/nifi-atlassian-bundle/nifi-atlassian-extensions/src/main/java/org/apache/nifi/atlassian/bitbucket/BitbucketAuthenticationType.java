/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.atlassian.bitbucket;

import org.apache.nifi.components.DescribedValue;

public enum BitbucketAuthenticationType implements DescribedValue {

    BASIC_AUTH("Basic Auth", """
            Username (not email) and App Password (https://support.atlassian.com/bitbucket-cloud/docs/app-passwords/).
            Or email and API Token (https://support.atlassian.com/bitbucket-cloud/docs/using-api-tokens/).
            Required permissions: repository, repository:read.
            """),

    ACCESS_TOKEN("Access Token", """
            Repository, Project or Workspace Token (https://support.atlassian.com/bitbucket-cloud/docs/access-tokens/).
            Required permissions: repository, repository:read.
            """),

    OAUTH2("OAuth 2.0", """
            Only works with client credentials flow which requires OAuth consumers
            (https://support.atlassian.com/bitbucket-cloud/docs/use-oauth-on-bitbucket-cloud/).
            OAuth consumer needs to be private, and callback url needs to be set, but can have any value.
            Other OAuth token service fields can be left empty or with default values. It is important to also note that
            the permissions/scopes set at the OAuth Consumer level are ignored and it will inherit the permissions of the
            owner of the OAuth Consumer.
            """);

    private final String displayName;
    private final String description;

    BitbucketAuthenticationType(final String displayName, final String description) {
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
