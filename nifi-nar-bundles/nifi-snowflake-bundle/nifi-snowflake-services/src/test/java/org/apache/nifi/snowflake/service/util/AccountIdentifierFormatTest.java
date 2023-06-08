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
package org.apache.nifi.snowflake.service.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AccountIdentifierFormatTest {

    @Test
    public void testAccountName() {
        final AccountIdentifierFormatParameters parameters = new AccountIdentifierFormatParameters(
                null,
                "ORG",
                "ACC",
                null,
                null,
                null);

        final String account = AccountIdentifierFormat.ACCOUNT_NAME.getAccount(parameters);
        final String accountUrl = AccountIdentifierFormat.ACCOUNT_NAME.getAccountUrl(parameters);

        assertEquals("ORG-ACC", account);
        assertEquals("ORG-ACC.snowflakecomputing.com", accountUrl);
    }

    @Test
    public void testAccountLocator() {
        final AccountIdentifierFormatParameters parameters = new AccountIdentifierFormatParameters(
                null,
                null,
                null,
                "LOC",
                "region",
                "cloud");

        final String account = AccountIdentifierFormat.ACCOUNT_LOCATOR.getAccount(parameters);
        final String accountUrl = AccountIdentifierFormat.ACCOUNT_LOCATOR.getAccountUrl(parameters);

        assertEquals("LOC", account);
        assertEquals("LOC.region.cloud.snowflakecomputing.com", accountUrl);
    }

    @Test
    public void testAccountUrl() {
        final AccountIdentifierFormatParameters parameters = new AccountIdentifierFormatParameters(
                "https://mysnowflake.mydomain.com",
                null,
                null,
                null,
                null,
                null);

        final String account = AccountIdentifierFormat.ACCOUNT_URL.getAccount(parameters);
        final String accountUrl = AccountIdentifierFormat.ACCOUNT_URL.getAccountUrl(parameters);

        assertEquals("mysnowflake", account);
        assertEquals("https://mysnowflake.mydomain.com", accountUrl);
    }

    @Test
    public void testAccountUrlWithHostname() {
        final AccountIdentifierFormatParameters parameters = new AccountIdentifierFormatParameters(
                "mysnowflake.mydomain.com",
                null,
                null,
                null,
                null,
                null);

        final String account = AccountIdentifierFormat.ACCOUNT_URL.getAccount(parameters);
        final String accountUrl = AccountIdentifierFormat.ACCOUNT_URL.getAccountUrl(parameters);

        assertEquals("mysnowflake", account);
        assertEquals("mysnowflake.mydomain.com", accountUrl);
    }
}
