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
package org.apache.nifi.util.locale;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Locale;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Testing of the test suite environment {@link java.util.Locale}.
 */
public class TestLocaleOfTestSuite {

    /**
     * Utility test that logs the {@link java.util.Locale} in which the project test suite is running.  The
     * assumptions validate the expected environment when run in Github CI.
     *
     * See also: "nifi/.github/workflows/ci-workflow.yml"
     */
    @Test
    public void testLocaleOfTestSuiteExecution() {
        final Logger logger = Logger.getLogger(getClass().getName());
        final String userLanguage = System.getProperty("user.language");
        final String userCountry = System.getProperty("user.country");
        final String userRegion = System.getProperty("user.region");
        final String userTimezone = System.getProperty("user.timezone");
        final String languageTag = Locale.getDefault().toLanguageTag();
        logger.info(String.format(
                "Test environment: locale=[%s] user.language=[%s], user.country=[%s], user.region=[%s], user.timezone=[%s]",
                languageTag, userLanguage, userCountry, userRegion, userTimezone));
        Assume.assumeTrue(Arrays.asList("en", "fr", "ja").contains(userLanguage));
        Assume.assumeTrue(Arrays.asList("AU", "FR", "JP").contains(userCountry));
        Assume.assumeTrue(Arrays.asList("en-AU", "fr-FR", "ja-JP").contains(languageTag));
    }

    /**
     * Some validation of expected outputs, given different {@link java.util.Locale} settings.  Limited to a few
     * locales for brevity.  The locales here are all absolute (no dependence on test environment).
     */
    @Test
    public void testLocaleExpectedOutputs() {
        Assert.assertEquals("1\u00a0000", NumberFormat.getInstance(Locale.FRANCE).format(1000));
        Assert.assertEquals("1\u00a0000", NumberFormat.getInstance(Locale.CANADA_FRENCH).format(1000));
        Assert.assertEquals("1\u00a0000", NumberFormat.getInstance(Locale.FRENCH).format(1000));
        Assert.assertEquals("1,000", NumberFormat.getInstance(Locale.US).format(1000));
        Assert.assertEquals("1,000", NumberFormat.getInstance(Locale.JAPAN).format(1000));

        Assert.assertEquals("E", DecimalFormatSymbols.getInstance(Locale.US).getExponentSeparator());
        Assert.assertEquals("E", DecimalFormatSymbols.getInstance(Locale.FRANCE).getExponentSeparator());
        Assert.assertEquals("E", DecimalFormatSymbols.getInstance(Locale.JAPAN).getExponentSeparator());
        Assert.assertEquals("E", DecimalFormatSymbols.getInstance(Locale.forLanguageTag("en-NZ")).getExponentSeparator());
        Assert.assertEquals("E", DecimalFormatSymbols.getInstance(Locale.forLanguageTag("fr-CH")).getExponentSeparator());
        Assert.assertEquals("E", DecimalFormatSymbols.getInstance(Locale.forLanguageTag("xx-YY")).getExponentSeparator());
    }

    /**
     * Document behavior of {@link DecimalFormatSymbols} in JRE on use of non-standard {@link Locale}.
     */
    @Test
    public void testExponentSeparatorByLocaleAndJavaVersion() {
        final String runtimeJavaVersion = System.getProperty("java.version");
        final boolean isJava8 = Pattern.compile("1\\.8.+").matcher(runtimeJavaVersion).matches();
        final String expected = (isJava8 ? "E" : "e");
        // used by project CI
        Assert.assertEquals(expected, DecimalFormatSymbols.getInstance(Locale.forLanguageTag("en-AU")).getExponentSeparator());
    }
}
