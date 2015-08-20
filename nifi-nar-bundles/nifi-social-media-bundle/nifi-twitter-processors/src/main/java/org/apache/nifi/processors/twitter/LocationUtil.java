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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility for parsing locations to be used with the Twitter API:
 *
 * https://dev.twitter.com/streaming/overview/request-parameters#locations
 *
 */
public class LocationUtil {

    static final String LON = "[-+]?\\d{1,3}(?:[.]\\d+)?";
    static final String LAT = "[-+]?\\d{1,2}(?:[.]\\d+)?";
    static final String LON_LAT = LON + ",\\s*" + LAT;
    static final String LOCATION = LON_LAT + ",\\s*" + LON_LAT;

    /**
     * Used to find locations one at a time after knowing the locations String is valid.
     */
    static final Pattern LOCATION_PATTERN = Pattern.compile(LOCATION);

    /**
     * One or more locations separated by a comma, exampple: lon,lat,lon,lat,lon,lat,lon,lat
     */
    static final Pattern LOCATIONS_PATTERN = Pattern.compile("(?:" + LOCATION + ")(?:,\\s*" + LOCATION + ")*");


    /**
     *
     * @param input a comma-separated list of longitude,latitude pairs specifying a set of bounding boxes,
     *              with the southwest corner of the bounding box coming first
     *
     * @return a list of the Location instances represented by the provided input
     */
    public static List<Location> parseLocations(final String input) {
        final List<Location> locations = new ArrayList<>();
        final Matcher locationsMatcher = LOCATIONS_PATTERN.matcher(input);
        if (locationsMatcher.matches()) {
            Matcher locationMatcher = LOCATION_PATTERN.matcher(input);
            while (locationMatcher.find()) {
                final String location = locationMatcher.group();
                locations.add(parseLocation(location));
            }
        } else {
            throw new IllegalStateException("The provided location string was invalid.");
        }

        return locations;
    }

    /**
     *
     * @param location a comma-separated list of longitude,latitude pairs specifying one location
     *
     * @return the Location instance for the provided input
     */
    public static Location parseLocation(final String location) {
        final String[] corSplit = location.split(",");

        final double swLon = Double.parseDouble(corSplit[0]) ;
        final double swLat = Double.parseDouble(corSplit[1]) ;

        final double neLon = Double.parseDouble(corSplit[2]) ;
        final double neLat = Double.parseDouble(corSplit[3]) ;

        Location.Coordinate sw = new Location.Coordinate(swLon, swLat) ;
        Location.Coordinate ne = new Location.Coordinate(neLon, neLat) ;
        return new Location(sw, ne) ;
    }

}
