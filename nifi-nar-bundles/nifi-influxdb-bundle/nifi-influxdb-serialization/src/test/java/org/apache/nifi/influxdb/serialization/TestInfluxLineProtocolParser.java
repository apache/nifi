/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.influxdb.serialization;

import avro.shaded.com.google.common.collect.MapDifference;
import avro.shaded.com.google.common.collect.Maps;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Stream;

/**
 * The tests comes from https://github.com/influxdata/influxdb/blob/1.6/models/points_test.go.
 */
public class TestInfluxLineProtocolParser {

    @Test
    public void parsePointNoValue() {

        ExpectedResult.fail().validate("");
    }

    @Test
    public void parsePointWhitespaceValue() {

        ExpectedResult.fail().validate(" ");
    }

    @Test
    public void parsePointNoFields() {

        String[] lineProtocols = {
                "cpu_load_short,host=server01,region=us-west",
                "cpu",
                "cpu,host==",
                "="
        };

        ExpectedResult.fail().validate(lineProtocols);
    }

    @Test
    public void parsePointNoTimestamp() {

        ExpectedResult.success()
                .measurement("cpu")
                .field("value", 1.0F)
                .validate("cpu value=1");
    }

    @Test
    public void parsePointMissingQuote() {

        // unbalanced quotes
        ExpectedResult.fail().validate("cpu,host=serverA value=\"test");

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .field("value", "test\"")
                .validate("cpu,host=serverA value=\"test\"\"");
    }

    @Test
    public void parsePointMissingTagKey() {

        String[] lineProtocols = {
                "cpu, value=1",
                "cpu,",
                "cpu,,,",
                "cpu,host=serverA,=us-east value=1i",
                "cpu,host=serverAa\\,,=us-east value=1i",
                "cpu,host=serverA\\,,=us-east value=1i",
                "cpu, =serverA value=1i"};

        ExpectedResult.fail().validate(lineProtocols);
    }

    @Test
    public void parsePointEscapedEmptyTagKey() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag(" ", "us-east")
                .field("value", 1L)
                .validate("cpu,host=serverA,\\ =us-east value=1i");
    }

    @Test
    public void parsePointMissingTagValue() {

        String[] lineProtocols = {
                "cpu,host",
                "cpu,host,",
                "cpu,host value=1i",
                "cpu,host=serverA,region value=1i",
                "cpu,host=serverA,region= value=1i",
                "cpu,host=serverA,region=,zone=us-west value=1i"};

        ExpectedResult.fail().validate(lineProtocols);
    }

    @Test
    public void parsePointInvalidTagFormat() {

        String[] lineProtocols = {
                "cpu,host=f=o,",
                "cpu,host=f\\==o,"};

        ExpectedResult.fail().validate(lineProtocols);
    }

    @Test
    public void parsePointMissingFieldName() {

        String[] lineProtocols = {
                "cpu,host=serverA,region=us-west =",
                "cpu,host=serverA,region=us-west =123i",
                "cpu,host=serverA,region=us-west value=123i,=456i"};

        ExpectedResult.fail().validate(lineProtocols);
    }

    @Test
    public void parsePointEscapedEmptyFieldKey() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-west")
                .field("a ", 123L)
                .validate("cpu,host=serverA,region=us-west a\\ =123i");
    }

    @Test
    public void parsePointMissingFieldValue() {

        String[] lineProtocols = {
                "cpu,host=serverA,region=us-west value=",
                "cpu,host=serverA,region=us-west value= 1000000000i",
                "cpu,host=serverA,region=us-west value=,value2=1i",
                "cpu,host=server01,region=us-west 1434055562000000000i",
                "cpu,host=server01,region=us-west value=1i,b"
        };

        ExpectedResult.fail().validate(lineProtocols);
    }

    @Test
    public void parsePointBadNumber() {

        String[] lineProtocols = {
                "cpu,host=serverA,region=us-west value=1a",
                "cpu,host=serverA,region=us-west value=1ii",
                "cpu,host=serverA,region=us-west value=1.0i"
        };

        ExpectedResult.fail().validate(lineProtocols);
    }

    @Test
    public void parsePointMaxInt64() {

        // over max
        ExpectedResult.fail()
                .validate("cpu,host=serverA,region=us-west value=9223372036854775808i");

        // max
        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-west")
                .field("value", 9223372036854775807L)
                .validate("cpu,host=serverA,region=us-west value=9223372036854775807i");

        // leading zeros
        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-west")
                .field("value", 9223372036854775807L)
                .validate("cpu,host=serverA,region=us-west value=0009223372036854775807i");
    }

    @Test
    public void parsePointMinInt64() {

        // under min
        ExpectedResult.fail()
                .validate("cpu,host=serverA,region=us-west value=-9223372036854775809i");

        // min
        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-west")
                .field("value", -9223372036854775808L)
                .validate("cpu,host=serverA,region=us-west value=-9223372036854775808i");

        // leading zeros
        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-west")
                .field("value", -9223372036854775808L)
                .validate("cpu,host=serverA,region=us-west value=-0009223372036854775808i");
    }

    @Test
    public void parsePointMaxFloat() {

        String maxValue = String.format("%.0f", Float.MAX_VALUE);

        // over max
        ExpectedResult.fail()
                .validate(String.format("cpu,host=serverA,region=us-west value=%s1", maxValue));

        // max
        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-west")
                .field("value", Float.MAX_VALUE)
                .validate(String.format("cpu,host=serverA,region=us-west value=%s", maxValue));

        // leading zeros
        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-west")
                .field("value", Float.MAX_VALUE)
                .validate(String.format("cpu,host=serverA,region=us-west value=0000%s", maxValue));
    }

    @Test
    public void parsePointMinFloat() {

        float maxValue = Float.MAX_VALUE;
        String maxValueFormatted = String.format("%.0f", maxValue);

        // under min
        ExpectedResult.fail()
                .validate(String.format("cpu,host=serverA,region=us-west value=-%s1", maxValueFormatted));

        // min
        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-west")
                .field("value", maxValue * -1)
                .validate(String.format("cpu,host=serverA,region=us-west value=-%s", maxValueFormatted));

        // leading zeros
        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-west")
                .field("value", maxValue * -1)
                .validate(String.format("cpu,host=serverA,region=us-west value=-0000%s", maxValueFormatted));
    }

    @Test
    public void parsePointNumberNonNumeric() {

        ExpectedResult.fail()
                .validate("cpu,host=serverA,region=us-west value=.1a");
    }

    @Test
    public void parsePointNegativeWrongPlace() {

        ExpectedResult.fail()
                .validate("cpu,host=serverA,region=us-west value=0.-1");
    }

    @Test
    public void parsePointOnlyNegativeSign() {

        ExpectedResult.fail()
                .validate("cpu,host=serverA,region=us-west value=-");
    }

    @Test
    public void parsePointFloatMultipleDecimals() {

        ExpectedResult.fail()
                .validate("cpu,host=serverA,region=us-west value=1.1.1");
    }

    @Test
    public void parsePointInteger() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-west")
                .field("value", 1L)
                .validate("cpu,host=serverA,region=us-west value=1i");
    }

    @Test
    public void parsePointNegativeInteger() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-west")
                .field("value", -1L)
                .validate("cpu,host=serverA,region=us-west value=-1i");
    }

    @Test
    public void parsePointNegativeFloat() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-west")
                .field("value", -1F)
                .validate("cpu,host=serverA,region=us-west value=-1.0");
    }

    @Test
    public void parsePointFloatNoLeadingDigit() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-west")
                .field("value", 0.1F)
                .validate("cpu,host=serverA,region=us-west value=.1");
    }

    @Test
    public void parsePointFloatScientific() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-west")
                .field("value", 1.0e4F)
                .validate("cpu,host=serverA,region=us-west value=1.0e4");

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-west")
                .field("value", 1e4F)
                .validate("cpu,host=serverA,region=us-west value=1e4");
    }

    @Test
    public void parsePointFloatScientificUpper() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-west")
                .field("value", 1.0e4F)
                .validate("cpu,host=serverA,region=us-west value=1.0E4");

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-west")
                .field("value", 1e4F)
                .validate("cpu,host=serverA,region=us-west value=1E4");
    }

    @Test
    public void parsePointFloatScientificDecimal() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-west")
                .field("value", 1.0e-4F)
                .validate("cpu,host=serverA,region=us-west value=1.0e-4");
    }

    @Test
    public void parsePointFloatNegativeScientific() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-west")
                .field("value", -1.0e-4F)
                .validate("cpu,host=serverA,region=us-west value=-1.0e-4");
    }

    @Test
    public void parsePointScientificIntInvalid() {

        ExpectedResult.fail()
                .validate("cpu,host=serverA,region=us-west value=9ie10");

        ExpectedResult.fail()
                .validate("cpu,host=serverA,region=us-west value=9e10i");
    }

    @Test
    public void parsePointBooleanInvalid() {

        ExpectedResult.fail()
                .validate("cpu,host=serverA,region=us-west value=a");
    }

    @Test
    public void parsePointUnescape() {

        // commas in measurement name
        ExpectedResult.success()
                .measurement("foo,bar")
                .field("value", 1L)
                .validate("foo\\,bar value=1i");

        // commas in tag value
        ExpectedResult.success()
                .measurement("cpu,main")
                .tag("regions", "east,west")
                .field("value", 1F)
                .validate("cpu\\,main,regions=east\\,west value=1.0");

        // spaces in measurement name
        ExpectedResult.success()
                .measurement("cpu load")
                .tag("region", "east")
                .field("value", 1F)
                .validate("cpu\\ load,region=east value=1.0");

        // equals in measurement name
        ExpectedResult.success()
                .measurement("cpu\\=load")
                .tag("region", "east")
                .field("value", 1F)
                .validate("cpu\\=load,region=east value=1.0");

        // equals in measurement name
        ExpectedResult.success()
                .measurement("cpu=load")
                .tag("region", "east")
                .field("value", 1F)
                .validate("cpu=load,region=east value=1.0");

        // commas in tag names
        ExpectedResult.success()
                .measurement("cpu")
                .tag("region,zone", "east")
                .field("value", 1F)
                .validate("cpu,region\\,zone=east value=1.0");

        // spaces in tag name
        ExpectedResult.success()
                .measurement("cpu")
                .tag("region zone", "east")
                .field("value", 1F)
                .validate("cpu,region\\ zone=east value=1.0");

        // space is tag name
        ExpectedResult.success()
                .measurement("cpu")
                .tag(" ", "east")
                .field("value", 1F)
                .validate("cpu,\\ =east value=1.0");

        // commas in tag values
        ExpectedResult.success()
                .measurement("cpu")
                .tag("regions", "east,west")
                .field("value", 1F)
                .validate("cpu,regions=east\\,west value=1.0");

        // spaces in tag values
        ExpectedResult.success()
                .measurement("cpu")
                .tag("regions", "east west")
                .field("value", 1F)
                .validate("cpu,regions=east\\ west value=1.0");

        // commas in field keys
        ExpectedResult.success()
                .measurement("cpu")
                .tag("regions", "east")
                .field("value,ms", 1F)
                .validate("cpu,regions=east value\\,ms=1.0");

        // spaces in field keys
        ExpectedResult.success()
                .measurement("cpu")
                .tag("regions", "east")
                .field("value ms", 1F)
                .validate("cpu,regions=east value\\ ms=1.0");

        // commas in field values
        ExpectedResult.success()
                .measurement("cpu")
                .tag("regions", "east")
                .field("value", "1,0")
                .validate("cpu,regions=east value=\"1,0\"");

        // random character escaped
        ExpectedResult.success()
                .measurement("cpu")
                .tag("regions", "eas\\t")
                .field("value", 1F)
                .validate("cpu,regions=eas\\t value=1.0");

        // backslash literal followed by escaped characters
        ExpectedResult.success()
                .measurement("cpu")
                .tag("regions", "\\,,=east")
                .field("value", 1F)
                .validate("cpu,regions=\\\\,\\,\\=east value=1.0");

        // field keys using escape char
        ExpectedResult.success()
                .measurement("cpu")
                .field("\\a", 1L)
                .validate("cpu \\a=1i");

        // measurement, tag and tag value with equals
        ExpectedResult.success()
                .measurement("cpu=load")
                .tag("equals=foo", "tag=value")
                .field("value", 1L)
                .validate("cpu=load,equals\\=foo=tag\\=value value=1i");

    }

    @Test
    public void parsePointWithTags() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-east")
                .field("value", 1F)
                .timestamp(1000000000L)
                .validate("cpu,host=serverA,region=us-east value=1.0 1000000000");
    }

    @Test
    public void parsePointWithDuplicateTags() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverB")
                .field("value", 1L)
                .timestamp(1000000000L)
                .validate("cpu,host=serverA,host=serverB value=1i 1000000000");
    }

    @Test
    public void parsePointWithStringField() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .field("value", "too\"hot\"")
                .validate("cpu,host=serverA value=\"too\\\"hot\\\"\"");

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-east")
                .field("value", 1F)
                .field("str", "foo")
                .field("str2", "bar")
                .timestamp(1000000000L)
                .validate("cpu,host=serverA,region=us-east value=1.0,str=\"foo\",str2=\"bar\" 1000000000");

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-east")
                .field("str", "foo \" bar")
                .timestamp(1000000000L)
                .validate("cpu,host=serverA,region=us-east str=\"foo \\\" bar\" 1000000000");
    }

    @Test
    public void parsePointWithStringWithSpaces() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-east")
                .field("value", 1F)
                .field("str", "foo bar")
                .timestamp(1000000000L)
                .validate("cpu,host=serverA,region=us-east value=1.0,str=\"foo bar\" 1000000000");
    }

    @Test
    public void parsePointWithStringWithNewline() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-east")
                .field("value", 1F)
                .field("str", "foo\\nbar")
                .timestamp(1000000000L)
                .validate("cpu,host=serverA,region=us-east value=1.0,str=\"foo\\nbar\" 1000000000");
    }

    @Test
    public void parsePointWithStringWithCommas() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-east")
                .field("value", 1F)
                .field("str", "foo\\,bar")
                .timestamp(1000000000L)
                .validate("cpu,host=serverA,region=us-east value=1.0,str=\"foo\\,bar\" 1000000000");

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-east")
                .field("value", 1F)
                .field("str", "foo,bar")
                .timestamp(1000000000L)
                .validate("cpu,host=serverA,region=us-east value=1.0,str=\"foo,bar\" 1000000000");
    }

    @Test
    public void parsePointQuotedMeasurement() {

        ExpectedResult.success()
                .measurement("\"cpu\"")
                .tag("host", "serverA")
                .tag("region", "us-east")
                .field("value", 1F)
                .timestamp(1000000000L)
                .validate("\"cpu\",host=serverA,region=us-east value=1.0 1000000000");
    }

    @Test
    public void parsePointQuotedTags() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("\"host\"", "\"serverA\"")
                .tag("region", "us-east")
                .field("value", 1F)
                .timestamp(1000000000L)
                .validate("cpu,\"host\"=\"serverA\",region=us-east value=1.0 1000000000");
    }

    @Test
    public void parsePointsUnbalancedQuotedTags() {

        ExpectedResult.fail()
                .validate("baz,mytag=\\\"a x=1 1441103862125\\nbaz,mytag=a z=1 1441103862126");
    }

    @Test
    public void parsePointEscapedStringsAndCommas() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-east")
                .field("value", "{Hello\"{,}\" World}")
                .timestamp(1000000000L)
                .validate("cpu,host=serverA,region=us-east value=\"{Hello\\\"{,}\\\" World}\" 1000000000");

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-east")
                .field("value", "{Hello\"{\\,}\" World}")
                .timestamp(1000000000L)
                .validate("cpu,host=serverA,region=us-east value=\"{Hello\\\"{\\,}\\\" World}\" 1000000000");
    }

    @Test
    public void parsePointWithStringWithEquals() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-east")
                .field("value", 1F)
                .field("str", "foo=bar")
                .timestamp(1000000000L)
                .validate("cpu,host=serverA,region=us-east str=\"foo=bar\",value=1.0 1000000000");
    }

    @Test
    public void parsePointWithStringWithBackslash() {

        ExpectedResult.success()
                .measurement("cpu")
                .field("value", "test\\\"")
                .timestamp(1000000000L)
                .validate("cpu value=\"test\\\\\\\"\" 1000000000");

        ExpectedResult.success()
                .measurement("cpu")
                .field("value", "test\\")
                .timestamp(1000000000L)
                .validate("cpu value=\"test\\\\\" 1000000000");

        ExpectedResult.success()
                .measurement("cpu")
                .field("value", "test\\\"")
                .timestamp(1000000000L)
                .validate("cpu value=\"test\\\\\\\"\" 1000000000");

        ExpectedResult.success()
                .measurement("cpu")
                .field("value", "test\"")
                .timestamp(1000000000L)
                .validate("cpu value=\"test\\\"\" 1000000000");

        ExpectedResult.success()
                .measurement("weather")
                .tag("location", "us-midwest")
                .field("temperature_str", "too hot/cold")
                .timestamp(1465839830100400201L)
                .validate("weather,location=us-midwest temperature_str=\"too hot/cold\" 1465839830100400201");

        ExpectedResult.success()
                .measurement("weather")
                .tag("location", "us-midwest")
                .field("temperature_str", "too hot\\cold")
                .timestamp(1465839830100400202L)
                .validate("weather,location=us-midwest temperature_str=\"too hot\\cold\" 1465839830100400202");

        ExpectedResult.success()
                .measurement("weather")
                .tag("location", "us-midwest")
                .field("temperature_str", "too hot\\cold")
                .timestamp(1465839830100400203L)
                .validate("weather,location=us-midwest temperature_str=\"too hot\\\\cold\" 1465839830100400203");

        ExpectedResult.success()
                .measurement("weather")
                .tag("location", "us-midwest")
                .field("temperature_str", "too hot\\\\cold")
                .timestamp(1465839830100400204L)
                .validate("weather,location=us-midwest temperature_str=\"too hot\\\\\\cold\" 1465839830100400204");

        ExpectedResult.success()
                .measurement("weather")
                .tag("location", "us-midwest")
                .field("temperature_str", "too hot\\\\cold")
                .timestamp(1465839830100400205L)
                .validate("weather,location=us-midwest temperature_str=\"too hot\\\\\\\\cold\" 1465839830100400205");

        ExpectedResult.success()
                .measurement("weather")
                .tag("location", "us-midwest")
                .field("temperature_str", "too hot\\\\\\cold")
                .timestamp(1465839830100400206L)
                .validate("weather,location=us-midwest temperature_str=\"too hot\\\\\\\\\\cold\" 1465839830100400206");
    }

    @Test
    public void parsePointWithBoolField() {

        String lineProtocol = "cpu,host=serverA,region=us-east true=true,t=t,T=T,TRUE=TRUE,True=True,false=false,"
                + "f=f,F=F,FALSE=FALSE,False=False 1000000000";

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-east")
                .field("true", true)
                .field("t", true)
                .field("T", true)
                .field("TRUE", true)
                .field("True", true)
                .field("false", false)
                .field("f", false)
                .field("F", false)
                .field("FALSE", false)
                .field("False", false)
                .timestamp(1000000000L)
                .validate(lineProtocol);
    }

    @Test
    public void parsePointUnicodeString() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-east")
                .field("value", "wè")
                .timestamp(1000000000L)
                .validate("cpu,host=serverA,region=us-east value=\"wè\" 1000000000");
    }

    @Test
    public void parsePointNegativeTimestamp() {

        ExpectedResult.success()
                .measurement("cpu")
                .field("value", 1F)
                .timestamp(-1L)
                .validate("cpu value=1 -1");
    }

    @Test
    public void parsePointMaxTimestamp() {

        ExpectedResult.success()
                .measurement("cpu")
                .field("value", 1F)
                .timestamp(9223372036854775807L)
                .validate("cpu value=1 9223372036854775807");
    }

    @Test
    public void parsePointMinTimestamp() {

        ExpectedResult.success()
                .measurement("cpu")
                .field("value", 1F)
                .timestamp(-9223372036854775807L)
                .validate("cpu value=1 -9223372036854775807");
    }

    @Test
    public void parsePointInvalidTimestamp() {

        String[] lineProtocols = {
                "cpu value=1 9223372036854775808",
                "cpu value=1 -92233720368547758078",
                "cpu value=1 -",
                "cpu value=1 -/",
                "cpu value=1 -1?",
                "cpu value=1 1-"
        };

        ExpectedResult.fail().validate(lineProtocols);
    }

    @Test
    public void newPointFloatWithoutDecimal() {

        ExpectedResult.success()
                .measurement("cpu")
                .field("value", 1F)
                .timestamp(1000000000L)
                .validate("cpu value=1 1000000000");
    }

    @Test
    public void newPointNegativeFloat() {

        ExpectedResult.success()
                .measurement("cpu")
                .field("value", -0.64F)
                .timestamp(1000000000L)
                .validate("cpu value=-0.64 1000000000");
    }

    @Test
    public void newPointFloatNoDecimal() {

        ExpectedResult.success()
                .measurement("cpu")
                .field("value", 1F)
                .timestamp(100000000L)
                .validate("cpu value=1. 100000000");
    }

    @Test
    public void newPointFloatScientific() {

        ExpectedResult.success()
                .measurement("cpu")
                .field("value", 6632243F)
                .timestamp(1000000000L)
                .validate("cpu value=6.632243e+06 1000000000");
    }

    @Test
    public void newPointLargeInteger() {

        ExpectedResult.success()
                .measurement("cpu")
                .field("value", 6632243L)
                .timestamp(1000000000L)
                .validate("cpu value=6632243i 1000000000");
    }

    @Test
    public void parsePointNaN() {

        String[] lineProtocols = {
                "cpu value=NaN 1000000000",
                "cpu value=nAn 1000000000",
                "cpu value=NaN"
        };

        ExpectedResult.fail().validate(lineProtocols);
    }

    @Test
    public void parsePointIntsFloats() {

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-east")
                .field("int", 10L)
                .field("float", 11F)
                .field("float2", 12.1F)
                .timestamp(1000000000L)
                .validate("cpu,host=serverA,region=us-east int=10i,float=11.0,float2=12.1 1000000000");
    }

    @Test
    public void parsePointToString() {

        String lineProtocol = "cpu,host=serverA,region=us-east "
                + "bool=false,float=11,float2=12.123,int=10i,str=\"string val\" 1000000000";

        ExpectedResult.success()
                .measurement("cpu")
                .tag("host", "serverA")
                .tag("region", "us-east")
                .field("bool", false)
                .field("float", 11F)
                .field("float2", 12.123F)
                .field("int", 10L)
                .field("str", "string val")
                .timestamp(1000000000L)
                .validate(lineProtocol);
    }

    @Test
    public void newPointEscaped() {

        ExpectedResult.success()
                .measurement("cpu,main")
                .tag("tag,bar", "value")
                .field("name,bar", 1F)
                .timestamp(0L)
                .validate("cpu\\,main,tag\\,bar=value name\\,bar=1 0");

        ExpectedResult.success()
                .measurement("cpu main")
                .tag("tag bar", "value")
                .field("name bar", 1F)
                .timestamp(0L)
                .validate("cpu\\ main,tag\\ bar=value name\\ bar=1 0");

        ExpectedResult.success()
                .measurement("cpu=main")
                .tag("tag=bar", "value=foo")
                .field("name=bar", 1F)
                .timestamp(0L)
                .validate("cpu=main,tag\\=bar=value\\=foo name\\=bar=1 0");
    }

    @Test
    public void newPointWithoutField() {

        ExpectedResult.fail().validate("cpu");
    }

    @Test
    public void newPointUnhandledType() {

        ExpectedResult.fail().validate("cpu value= 0");

        ExpectedResult.success()
                .measurement("cpu")
                .field("value", "1970-01-01 00:00:00 +0000 UTC")
                .timestamp(0L)
                .validate("cpu value=\"1970-01-01 00:00:00 +0000 UTC\" 0");
    }

    @Test
    public void parsePointsStringWithExtraBuffer() {

        ExpectedResult.fail().validate("cpu \"a=1\"=2");
    }

    @Test
    public void parsePointsQuotesInTags() {

        ExpectedResult.success()
                .measurement("t159")
                .tag("label", "hey \"ya")
                .field("a", 1L)
                .field("value", 0L)
                .validate("t159,label=hey\\ \"ya a=1i,value=0i");

        ExpectedResult.success()
                .measurement("t159")
                .tag("label", "another")
                .field("a", 2L)
                .field("value", 1L)
                .timestamp(1L)
                .validate("t159,label=another a=2i,value=1i 1");
    }

    private static final class ExpectedResult {

        private static final Logger LOG = LoggerFactory.getLogger(ExpectedResult.class);

        private final Boolean error;
        private String measurement;
        private Map<String, String> tags = Maps.newHashMap();
        private Map<String, Object> fields = Maps.newHashMap();
        private Long timestamp;

        private ExpectedResult(@NonNull final Boolean error) {
            this.error = error;
        }

        @NonNull
        public static ExpectedResult success() {
            return new ExpectedResult(false);
        }

        @NonNull
        public static ExpectedResult fail() {
            return new ExpectedResult(true);
        }

        @NonNull
        public ExpectedResult measurement(@NonNull final String measurement) {
            this.measurement = measurement;

            return this;
        }

        @NonNull
        public ExpectedResult tag(@NonNull final String key, @NonNull final String value) {
            this.tags.put(key, value);

            return this;
        }

        @NonNull
        public ExpectedResult field(@NonNull final String key, @NonNull final Object value) {
            this.fields.put(key, value);

            return this;
        }

        @NonNull
        public ExpectedResult timestamp(@Nullable final Long timestamp) {
            this.timestamp = timestamp;

            return this;
        }

        private void validate(@NonNull final String... lineProtocols) {

            Assert.assertNotNull(lineProtocols);

            Stream.of(lineProtocols).forEach(lineProtocol -> {

                InfluxLineProtocolParser parse = null;
                try {
                    parse = InfluxLineProtocolParser.parse(lineProtocol).report();

                } catch (NotParsableInlineProtocolData notParsableInlineProtocolData) {

                    if (!error) {
                        LOG.error("StackTrace: ", notParsableInlineProtocolData);

                        Assert.fail("Not expected error: " + notParsableInlineProtocolData);
                    }

                    return;
                }

                Assert.assertFalse("Expected fail for: " + lineProtocol, error);

                Assert.assertEquals(measurement, parse.getMeasurement());

                MapDifference<String, Object> tagsDif = Maps.difference(tags, parse.getTags());
                Assert.assertTrue("Tags has incorrect values: " + tagsDif.toString(), tagsDif.areEqual());

                MapDifference<String, Object> fieldsDif = Maps.difference(fields, parse.getFields());
                Assert.assertTrue("Fields has incorrect values: " + fieldsDif.toString(), fieldsDif.areEqual());

                Assert.assertEquals(timestamp, parse.getTimestamp());
            });
        }
    }
}
