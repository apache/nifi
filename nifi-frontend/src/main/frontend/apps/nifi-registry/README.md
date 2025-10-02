<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# NiFi Registry LDAP setup

This document contains instructions for testing the NiFi Registry LDAP Identity Provider and LDAP User Group Provider.

It utilizes a publicly accessible LDAP test server, made available by Forum Systems. For more information on this server, see:

http://www.forumsys.com/tutorials/integration-how-to/ldap/online-ldap-test-server/

## Configuring NiFi Registry for HTTPS

nifi-registry.properties is located in nifi-registry/nifi-registry-assembly/target/nifi-registry-2.7.0-SNAPSHOT-bin/nifi-registry-2.7.0-SNAPSHOT/conf/. You will need to update that file to configure the nifi registry to run securely with LDAP.

For running a NiFi Registry on localhost, you should already have test keys and certs in your locally built clone of the nifi registry source code repository:

    /path/to/nifi/nifi-registry/nifi-registry-core/nifi-registry-web-api/src/test/resources/keys
      +-- registry-ks.jks
      +-- ca-ts.jks

You need to copy those files into nifi-registry/nifi-registry-assembly/target/nifi-registry-2.7.0-SNAPSHOT-bin/nifi-registry-2.7.0-SNAPSHOT/conf/.

The NiFi Registry Server will need to know about the key store, which has its key/cert pair, and the trust store, which has the certificate for the root CA that generated the key/cert pair. To configure this, set the following properties in your nifi-registry.properties file:

    nifi.registry.web.https.host=localhost
    nifi.registry.web.https.port=18443

    nifi.registry.security.keystore=./conf/registry-ks.jks
    nifi.registry.security.keystoreType=JKS
    nifi.registry.security.keystorePasswd=password
    nifi.registry.security.keyPasswd=password
    nifi.registry.security.truststore=./conf/ca-ts.jks
    nifi.registry.security.truststoreType=JKS
    nifi.registry.security.truststorePasswd=password
    nifi.registry.security.needClientAuth=false
    nifi.registry.security.authorizers.configuration.file=./conf/authorizers.xml
    nifi.registry.security.authorizer=managed-authorizer
    nifi.registry.security.identity.providers.configuration.file=./conf/identity-providers.xml
    nifi.registry.security.identity.provider=ldap-identity-provider

This will make the NiFi Registry available at https://localhost:18443.

Now stop any running nifi registy you have running locally. In nifi-registry/nifi-registry-assembly/target/nifi-registry-2.7.0-SNAPSHOT-bin/nifi-registry-2.7.0-SNAPSHOT/conf/ update the authorizers.xml and identity-providers.xml with the following:

### (authorizers.xml)

```
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<authorizers>

    <!--
        The LdapUserGroupProvider will retrieve users and groups from an LDAP server. The users and groups
        are not configurable.

        'Authentication Strategy' - How the connection to the LDAP server is authenticated. Possible
            values are ANONYMOUS, SIMPLE, LDAPS, or START_TLS.

        'Manager DN' - The DN of the manager that is used to bind to the LDAP server to search for users.
        'Manager Password' - The password of the manager that is used to bind to the LDAP server to
            search for users.

        'TLS - Keystore' - Path to the Keystore that is used when connecting to LDAP using LDAPS or START_TLS.
        'TLS - Keystore Password' - Password for the Keystore that is used when connecting to LDAP
            using LDAPS or START_TLS.
        'TLS - Keystore Type' - Type of the Keystore that is used when connecting to LDAP using
            LDAPS or START_TLS (i.e. JKS or PKCS12).
        'TLS - Truststore' - Path to the Truststore that is used when connecting to LDAP using LDAPS or START_TLS.
        'TLS - Truststore Password' - Password for the Truststore that is used when connecting to
            LDAP using LDAPS or START_TLS.
        'TLS - Truststore Type' - Type of the Truststore that is used when connecting to LDAP using
            LDAPS or START_TLS (i.e. JKS or PKCS12).
        'TLS - Client Auth' - Client authentication policy when connecting to LDAP using LDAPS or START_TLS.
            Possible values are REQUIRED, WANT, NONE.
        'TLS - Protocol' - Protocol to use when connecting to LDAP using LDAPS or START_TLS. (i.e. TLS,
            TLSv1.1, TLSv1.2, etc).
        'TLS - Shutdown Gracefully' - Specifies whether the TLS should be shut down gracefully
            before the target context is closed. Defaults to false.

        'Referral Strategy' - Strategy for handling referrals. Possible values are FOLLOW, IGNORE, THROW.
        'Connect Timeout' - Duration of connect timeout. (i.e. 10 secs).
        'Read Timeout' - Duration of read timeout. (i.e. 10 secs).

        'Url' - Space-separated list of URLs of the LDAP servers (i.e. ldap://<hostname>:<port>).
        'Page Size' - Sets the page size when retrieving users and groups. If not specified, no paging is performed.
        'Sync Interval' - Duration of time between syncing users and groups. (i.e. 30 mins).

        'User Search Base' - Base DN for searching for users (i.e. ou=users,o=nifi). Required to search users.
        'User Object Class' - Object class for identifying users (i.e. person). Required if searching users.
        'User Search Scope' - Search scope for searching users (ONE_LEVEL, OBJECT, or SUBTREE). Required if searching users.
        'User Search Filter' - Filter for searching for users against the 'User Search Base' (i.e. (memberof=cn=team1,ou=groups,o=nifi) ). Optional.
        'User Identity Attribute' - Attribute to use to extract user identity (i.e. cn). Optional. If not set, the entire DN is used.
        'User Group Name Attribute' - Attribute to use to define group membership (i.e. memberof). Optional. If not set
            group membership will not be calculated through the users. Will rely on group membership being defined
            through 'Group Member Attribute' if set.

        'Group Search Base' - Base DN for searching for groups (i.e. ou=groups,o=nifi). Required to search groups.
        'Group Object Class' - Object class for identifying groups (i.e. groupOfNames). Required if searching groups.
        'Group Search Scope' - Search scope for searching groups (ONE_LEVEL, OBJECT, or SUBTREE). Required if searching groups.
        'Group Search Filter' - Filter for searching for groups against the 'Group Search Base'. Optional.
        'Group Name Attribute' - Attribute to use to extract group name (i.e. cn). Optional. If not set, the entire DN is used.
        'Group Member Attribute' - Attribute to use to define group membership (i.e. member). Optional. If not set
            group membership will not be calculated through the groups. Will rely on group member being defined
            through 'User Group Name Attribute' if set.

        NOTE: Any identity mapping rules specified in nifi-registry.properties will also be applied to the user identities.
            Group names are not mapped.
    -->
    <userGroupProvider>
        <identifier>ldap-user-group-provider</identifier>
        <class>org.apache.nifi.registry.security.ldap.tenants.LdapUserGroupProvider</class>
        <property name="Authentication Strategy">SIMPLE</property>

        <property name="Manager DN">cn=read-only-admin,dc=example,dc=com</property>
        <property name="Manager Password">password</property>

        <!--
        <property name="TLS - Keystore"></property>
        <property name="TLS - Keystore Password"></property>
        <property name="TLS - Keystore Type"></property>
        <property name="TLS - Truststore"></property>
        <property name="TLS - Truststore Password"></property>
        <property name="TLS - Truststore Type"></property>
        <property name="TLS - Client Auth"></property>
        <property name="TLS - Protocol"></property>
        <property name="TLS - Shutdown Gracefully"></property>
        -->

        <property name="Referral Strategy">FOLLOW</property>
        <property name="Connect Timeout">10 secs</property>
        <property name="Read Timeout">10 secs</property>

        <property name="Url">ldap://ldap.forumsys.com:389</property>
        <!--<property name="Page Size"></property>-->
        <property name="Sync Interval">30 mins</property>

        <property name="User Search Base">dc=example,dc=com</property>
        <property name="User Object Class">person</property>
        <property name="User Search Scope">ONE_LEVEL</property>
        <property name="User Search Filter">(uid=*)</property>
        <property name="User Identity Attribute">uid</property>
        <!--<property name="User Group Name Attribute">ou</property>-->

        <property name="Group Search Base">dc=example,dc=com</property>
        <property name="Group Object Class">groupOfUniqueNames</property>
        <property name="Group Search Scope">ONE_LEVEL</property>
        <property name="Group Search Filter">(ou=*)</property>
        <property name="Group Name Attribute">ou</property>
        <property name="Group Member Attribute">uniqueMember</property>
    </userGroupProvider>

    <!--
        The FileAccessPolicyProvider will provide support for managing access policies which is backed by a file
        on the local file system.

        - User Group Provider - The identifier for an User Group Provider defined above that will be used to access
            users and groups for use in the managed access policies.

        - Authorizations File - The file where the FileAccessPolicyProvider will store policies.

        - Initial Admin Identity - The identity of an initial admin user that will be granted access to the UI and
            given the ability to create additional users, groups, and policies. The value of this property could be
            a DN when using certificates or LDAP. This property will only be used when there
            are no other policies defined.

            NOTE: Any identity mapping rules specified in nifi-registry.properties will also be applied to the initial admin identity,
            so the value should be the unmapped identity. This identity must be found in the configured User Group Provider.

        - NiFi Identity [unique key] - The identity of a NiFi node that will have access to this NiFi Registry and will be able
            to act as a proxy on behalf of a NiFi Registry end user. A property should be created for the identity of every NiFi
            node that needs to access this NiFi Registry. The name of each property must be unique, for example for three
            NiFi clients:
            "NiFi Identity A", "NiFi Identity B", "NiFi Identity C" or "NiFi Identity 1", "NiFi Identity 2", "NiFi Identity 3"

            NOTE: Any identity mapping rules specified in nifi-registry.properties will also be applied to the nifi identities,
            so the values should be the unmapped identities (i.e. full DN from a certificate). This identity must be found
            in the configured User Group Provider.
    -->
    <accessPolicyProvider>
        <identifier>file-access-policy-provider</identifier>
        <class>org.apache.nifi.registry.security.authorization.file.FileAccessPolicyProvider</class>
        <property name="User Group Provider">ldap-user-group-provider</property>
        <property name="Authorizations File">./conf/authorizations.xml</property>
        <property name="Initial Admin Identity">nobel</property>

        <!--<property name="NiFi Identity 1"></property>-->
    </accessPolicyProvider>

    <!--
        The StandardManagedAuthorizer. This authorizer implementation must be configured with the
        Access Policy Provider which it will use to access and manage users, groups, and policies.
        These users, groups, and policies will be used to make all access decisions during authorization
        requests.

        - Access Policy Provider - The identifier for an Access Policy Provider defined above.
    -->
    <authorizer>
        <identifier>managed-authorizer</identifier>
        <class>org.apache.nifi.registry.security.authorization.StandardManagedAuthorizer</class>
        <property name="Access Policy Provider">file-access-policy-provider</property>
    </authorizer>

</authorizers>
```

### (identity-providers.xml)

```
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<identityProviders>
    <!--
        Identity Provider for users logging in with username/password against an LDAP server.

        'Authentication Strategy' - How the connection to the LDAP server is authenticated. Possible
            values are ANONYMOUS, SIMPLE, LDAPS, or START_TLS.

        'Manager DN' - The DN of the manager that is used to bind to the LDAP server to search for users.
        'Manager Password' - The password of the manager that is used to bind to the LDAP server to
            search for users.

        'TLS - Keystore' - Path to the Keystore that is used when connecting to LDAP using LDAPS or START_TLS.
        'TLS - Keystore Password' - Password for the Keystore that is used when connecting to LDAP
            using LDAPS or START_TLS.
        'TLS - Keystore Type' - Type of the Keystore that is used when connecting to LDAP using
            LDAPS or START_TLS (i.e. JKS or PKCS12).
        'TLS - Truststore' - Path to the Truststore that is used when connecting to LDAP using LDAPS or START_TLS.
        'TLS - Truststore Password' - Password for the Truststore that is used when connecting to
            LDAP using LDAPS or START_TLS.
        'TLS - Truststore Type' - Type of the Truststore that is used when connecting to LDAP using
            LDAPS or START_TLS (i.e. JKS or PKCS12).
        'TLS - Client Auth' - Client authentication policy when connecting to LDAP using LDAPS or START_TLS.
            Possible values are REQUIRED, WANT, NONE.
        'TLS - Protocol' - Protocol to use when connecting to LDAP using LDAPS or START_TLS. (i.e. TLS,
            TLSv1.1, TLSv1.2, etc).
        'TLS - Shutdown Gracefully' - Specifies whether the TLS should be shut down gracefully
            before the target context is closed. Defaults to false.

        'Referral Strategy' - Strategy for handling referrals. Possible values are FOLLOW, IGNORE, THROW.
        'Connect Timeout' - Duration of connect timeout. (i.e. 10 secs).
        'Read Timeout' - Duration of read timeout. (i.e. 10 secs).

        'Url' - Space-separated list of URLs of the LDAP servers (i.e. ldap://<hostname>:<port>).
        'User Search Base' - Base DN for searching for users (i.e. CN=Users,DC=example,DC=com).
        'User Search Filter' - Filter for searching for users against the 'User Search Base'.
            (i.e. sAMAccountName={0}). The user specified name is inserted into '{0}'.

        'Identity Strategy' - Strategy to identify users. Possible values are USE_DN and USE_USERNAME.
            The default functionality if this property is missing is USE_DN in order to retain
            backward compatibility. USE_DN will use the full DN of the user entry if possible.
            USE_USERNAME will use the username the user logged in with.
        'Authentication Expiration' - The duration of how long the user authentication is valid
            for. If the user never logs out, they will be required to log back in following
            this duration.
    -->
    <provider>
        <identifier>ldap-identity-provider</identifier>
        <class>org.apache.nifi.registry.security.ldap.LdapIdentityProvider</class>
        <property name="Authentication Strategy">SIMPLE</property>

        <property name="Manager DN">cn=read-only-admin,dc=example,dc=com</property>
        <property name="Manager Password">password</property>

        <property name="Referral Strategy">FOLLOW</property>
        <property name="Connect Timeout">10 secs</property>
        <property name="Read Timeout">10 secs</property>

        <property name="Url">ldap://ldap.forumsys.com:389</property>
        <property name="User Search Base">dc=example,dc=com</property>
        <property name="User Search Filter">(uid={0})</property>

        <!--<property name="Identity Strategy">USE_DN</property>-->
        <property name="Identity Strategy">USE_USERNAME</property>
        <property name="Authentication Expiration">12 hours</property>
    </provider>

</identityProviders>
```

Now delete the users.xml file if you have onw. It will get recreated when you restart the nifi registry backend.

Next, update your nifi-frontend/src/main/frontend/apps/nifi-registry/proxy.config.mjs so that when you start your local frontend dev server it will proxy through to the secured nifi registry backend:

```
const target = {
    target: 'https://localhost:18443',
    secure: false,
    logLevel: 'debug',
    changeOrigin: true,
    headers: {
        'X-ProxyScheme': 'http',
        'X-ProxyPort': 4204
    },
    configure: (proxy, _options) => {
        proxy.on('error', (err, _req, _res) => {
            console.log('proxy error', err);
        });
        proxy.on('proxyReq', (proxyReq, req, _res) => {
            console.log('Sending Request to the Target:', req.method, req.url);
        });
        proxy.on('proxyRes', (proxyRes, req, _res) => {
            console.log('Received Response from the Target:', proxyRes.statusCode, req.url);
        });
    },
    bypass: function (req) {
        if (req.url.startsWith('/nifi-registry/')) {
            return req.url;
        }
    }
};

export default {
    '/**': target
};

```

Now start the nifi registry backend again. You should be able to log in with one of the test users listed on the forumsys.com page, such as:

    User: nobel
    Password: password

Done!!! Good job!
