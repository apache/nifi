#!/bin/sh -e

#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

prop_replace 'nifi.security.user.oidc.discovery.url'                    "${NIFI_SECURITY_USER_OIDC_DISCOVERY_URL}"
prop_replace 'nifi.security.user.oidc.connect.timeout'                  "${NIFI_SECURITY_USER_OIDC_CONNECT_TIMEOUT}"
prop_replace 'nifi.security.user.oidc.read.timeout'                     "${NIFI_SECURITY_USER_OIDC_READ_TIMEOUT}"
prop_replace 'nifi.security.user.oidc.client.id'                        "${NIFI_SECURITY_USER_OIDC_CLIENT_ID}"
prop_replace 'nifi.security.user.oidc.client.secret'                    "${NIFI_SECURITY_USER_OIDC_CLIENT_SECRET}"
prop_replace 'nifi.security.user.oidc.preferred.jwsalgorithm'           "${NIFI_SECURITY_USER_OIDC_PREFERRED_JWSALGORITHM}"
prop_replace 'nifi.security.user.oidc.additional.scopes'                "${NIFI_SECURITY_USER_OIDC_ADDITIONAL_SCOPES}"
prop_replace 'nifi.security.user.oidc.claim.identifying.user'           "${NIFI_SECURITY_USER_OIDC_CLAIM_IDENTIFYING_USER}"
prop_replace 'nifi.security.user.oidc.fallback.claims.identifying.user' "${NIFI_SECURITY_USER_OIDC_FALLBACK_CLAIMS_IDENTIFYING_USER}"
prop_replace 'nifi.security.user.oidc.truststore.strategy'              "${NIFI_SECURITY_USER_OIDC_TRUSTSTORE_STRATEGY}"
