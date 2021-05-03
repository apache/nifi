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
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Testing of the test suite environment {@link java.util.Locale}.  The locales specified
 * in ".github/workflows/ci-workflow.yml" are exercised.  This test is inert when run in alternate locales.
 */
public class TestLocaleOfTestSuite {

    /**
     * Test behaviors associated with non-standard ".github/workflows/ci-workflow.yml" {@link java.util.Locale} "en-AU".
     */
    @Test
    public void testLocaleCI_EN_AU() {
        final Locale locale = Locale.getDefault();
        Assume.assumeTrue(locale.toLanguageTag().equals("en-AU"));

        final String runtimeJavaVersion = System.getProperty("java.version");
        final boolean isJava8 = Pattern.compile("1\\.8.+").matcher(runtimeJavaVersion).matches();
        final String expected = (isJava8 ? "E" : "e");  // tested in Java 8 and Java 11

        Assert.assertEquals(expected, DecimalFormatSymbols.getInstance(locale).getExponentSeparator());
        Assert.assertEquals("1,000", NumberFormat.getInstance(locale).format(1000));
    }

    /**
     * Test behaviors associated with ".github/workflows/ci-workflow.yml" {@link java.util.Locale#JAPAN}.
     */
    @Test
    public void testLocaleCI_JA_JP() {
        final Locale locale = Locale.getDefault();
        Assume.assumeTrue(locale.toLanguageTag().equals("ja-JP"));

        Assert.assertEquals("E", DecimalFormatSymbols.getInstance(locale).getExponentSeparator());
        Assert.assertEquals("1,000", NumberFormat.getInstance(locale).format(1000));
    }

    /**
     * Test behaviors associated with ".github/workflows/ci-workflow.yml" {@link java.util.Locale#FRANCE}.
     */
    @Test
    public void testLocaleCI_FR_FR() {
        final Locale locale = Locale.getDefault();
        Assume.assumeTrue(locale.toLanguageTag().equals("fr-FR"));

        Assert.assertEquals("E", DecimalFormatSymbols.getInstance(locale).getExponentSeparator());
        Assert.assertEquals("1\u00a0000", NumberFormat.getInstance(locale).format(1000));
    }
}
