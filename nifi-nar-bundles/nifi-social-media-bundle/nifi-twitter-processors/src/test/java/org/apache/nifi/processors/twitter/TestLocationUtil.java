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
package org.apache.nifi.processors.twitter;

import com.twitter.hbc.core.endpoint.Location;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestLocationUtil {

    @Test
    public void testParseLocationsSingle() {
        final String swLon = "-122.75";
        final String swLat = "36.8";
        final String neLon = "-121.75";
        final String neLat = "37.8";

        final String locationString = swLon + "," + swLat + "," + neLon + "," + neLat;
        List<Location> locations = LocationUtil.parseLocations(locationString);
        Assert.assertEquals(1, locations.size());

        Location location = locations.get(0);
        Assert.assertEquals(new Double(location.southwestCoordinate().longitude()), Double.valueOf(swLon));
        Assert.assertEquals(new Double(location.southwestCoordinate().latitude()), Double.valueOf(swLat));
        Assert.assertEquals(new Double(location.northeastCoordinate().longitude()), Double.valueOf(neLon));
        Assert.assertEquals(new Double(location.northeastCoordinate().latitude()), Double.valueOf(neLat));
    }

    @Test
    public void testParseLocationsMultiple() {
        final Location location1 = new Location(new Location.Coordinate(-122.75, 36.8), new Location.Coordinate(-121.75,37.8));
        final Location location2 = new Location(new Location.Coordinate(-74, 40), new Location.Coordinate(-73, 41));
        final Location location3 = new Location(new Location.Coordinate(-64, 30), new Location.Coordinate(-63, 31));
        final Location location4 = new Location(new Location.Coordinate(-54, 20), new Location.Coordinate(-53, 21));

        final List<Location> expectedLocations = Arrays.asList(location1, location2, location3, location4);

        final String locationString = "-122.75,36.8,-121.75,37.8,-74,40,-73,41,-64,30,-63,31,-54,20,-53,21";
        List<Location> locations = LocationUtil.parseLocations(locationString);
        Assert.assertEquals(expectedLocations.size(), locations.size());

        for (Location expectedLocation : expectedLocations) {
            boolean found = false;
            for (Location location : locations) {
                if (location.northeastCoordinate().longitude() == expectedLocation.northeastCoordinate().longitude()
                        && location.northeastCoordinate().latitude() == expectedLocation.northeastCoordinate().latitude()
                        && location.southwestCoordinate().longitude() == expectedLocation.southwestCoordinate().longitude()
                        && location.southwestCoordinate().latitude() == expectedLocation.southwestCoordinate().latitude()) {
                    found = true;
                    break;
                }
            }
            Assert.assertTrue(found);
        }

    }
}
