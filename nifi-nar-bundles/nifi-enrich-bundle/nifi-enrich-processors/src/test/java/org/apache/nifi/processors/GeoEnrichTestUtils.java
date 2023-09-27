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
package org.apache.nifi.processors;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jr.ob.JSON;
import com.maxmind.geoip2.model.CityResponse;
import java.util.Collections;

public class GeoEnrichTestUtils {
    public static CityResponse getFullCityResponse() throws Exception {
        // Taken from MaxMind unit tests.
        final String maxMindCityResponse = JSON.std
                .composeString()
                .startObject()
                .startObjectField("maxmind")
                .put("queries_remaining", 11)
                .end()
                .startObjectField("registered_country")
                .put("geoname_id", 2)
                .startObjectField("names")
                .put("en", "Canada")
                .end()
                .put("is_in_european_union", false)
                .put("iso_code", "CA")
                .end()
                .startObjectField("traits")
                .put("is_anonymous_proxy", true)
                .put("autonomous_system_number", 1234)
                .put("isp", "Comcast")
                .put("ip_address", "1.2.3.4")
                .put("is_satellite_provider", true)
                .put("autonomous_system_organization", "AS Organization")
                .put("organization", "Blorg")
                .put("domain", "example.com")
                // These are here just to simplify the testing. We expect the
                // difference
                .put("is_anonymous", false)
                .put("is_anonymous_vpn", false)
                .put("is_hosting_provider", false)
                .put("is_legitimate_proxy", false)
                .put("is_public_proxy", false)
                .put("is_residential_proxy", false)
                .put("is_tor_exit_node", false)
                .put("network", "1.2.3.0/24")
                .end()
                .startObjectField("country")
                .startObjectField("names")
                .put("en", "United States of America")
                .end()
                .put("geoname_id", 1)
                .put("is_in_european_union", false)
                .put("iso_code", "US")
                .end()
                .startObjectField("continent")
                .startObjectField("names")
                .put("en", "North America")
                .end()
                .put("code", "NA")
                .put("geoname_id", 42)
                .end()
                .startObjectField("location")
                .put("time_zone", "America/Chicago")
                .put("metro_code", 765)
                .put("latitude", 44.98)
                .put("longitude", 93.2636)
                .end()
                .startArrayField("subdivisions")
                .startObject()
                .put("iso_code", "MN")
                .put("geoname_id", 574635)
                .startObjectField("names")
                .put("en", "Minnesota")
                .end()
                .end()
                .startObject()
                .put("iso_code", "TT")
                .end()
                .end()
                .startObjectField("represented_country")
                .put("geoname_id", 3)
                .startObjectField("names")
                .put("en", "United Kingdom")
                .end()
                .put("type", "C<military>")
                .put("is_in_european_union", true)
                .put("iso_code", "GB")
                .end()
                .startObjectField("postal")
                .put("code", "55401")
                .end()
                .startObjectField("city")
                .put("geoname_id", 9876)
                .startObjectField("names")
                .put("en", "Minneapolis")
                .end()
                .end()
                .end()
                .finish();

        InjectableValues inject = new InjectableValues.Std().addValue("locales", Collections.singletonList("en"));
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        return new ObjectMapper().readerFor(CityResponse.class).with(inject).readValue(maxMindCityResponse);
    }

    public static CityResponse getNullLatAndLongCityResponse() throws Exception {
        // Taken from MaxMind unit tests and modified.
        final String maxMindCityResponse = JSON.std
                .composeString()
                .startObject()
                .startObjectField("maxmind")
                .put("queries_remaining", 11)
                .end()
                .startObjectField("registered_country")
                .put("geoname_id", 2)
                .startObjectField("names")
                .put("en", "Canada")
                .end()
                .put("is_in_european_union", false)
                .put("iso_code", "CA")
                .end()
                .startObjectField("traits")
                .put("is_anonymous_proxy", true)
                .put("autonomous_system_number", 1234)
                .put("isp", "Comcast")
                .put("ip_address", "1.2.3.4")
                .put("is_satellite_provider", true)
                .put("autonomous_system_organization", "AS Organization")
                .put("organization", "Blorg")
                .put("domain", "example.com")
                // These are here just to simplify the testing. We expect the
                // difference
                .put("is_anonymous", false)
                .put("is_anonymous_vpn", false)
                .put("is_hosting_provider", false)
                .put("is_legitimate_proxy", false)
                .put("is_public_proxy", false)
                .put("is_residential_proxy", false)
                .put("is_tor_exit_node", false)
                .put("network", "1.2.3.0/24")
                .end()
                .startObjectField("country")
                .startObjectField("names")
                .put("en", "United States of America")
                .end()
                .put("geoname_id", 1)
                .put("is_in_european_union", false)
                .put("iso_code", "US")
                .end()
                .startObjectField("continent")
                .startObjectField("names")
                .put("en", "North America")
                .end()
                .put("code", "NA")
                .put("geoname_id", 42)
                .end()
                .startObjectField("location")
                .put("time_zone", "America/Chicago")
                .put("metro_code", 765)
                .end()
                .startArrayField("subdivisions")
                .startObject()
                .put("iso_code", "MN")
                .put("geoname_id", 574635)
                .startObjectField("names")
                .put("en", "Minnesota")
                .end()
                .end()
                .startObject()
                .put("iso_code", "TT")
                .end()
                .end()
                .startObjectField("represented_country")
                .put("geoname_id", 3)
                .startObjectField("names")
                .put("en", "United Kingdom")
                .end()
                .put("type", "C<military>")
                .put("is_in_european_union", true)
                .put("iso_code", "GB")
                .end()
                .startObjectField("postal")
                .put("code", "55401")
                .end()
                .startObjectField("city")
                .put("geoname_id", 9876)
                .startObjectField("names")
                .put("en", "Minneapolis")
                .end()
                .end()
                .end()
                .finish();

        InjectableValues inject = new InjectableValues.Std().addValue("locales", Collections.singletonList("en"));
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        return new ObjectMapper().readerFor(CityResponse.class).with(inject).readValue(maxMindCityResponse);
    }
}
