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
package org.apache.nifi.processors.hadoop.inotify;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.Test;

import java.util.regex.Pattern;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestNotificationEventPathFilter {

    @Test
    public void acceptShouldProperlyReturnFalseWithNullPath() throws Exception {
        assertFalse(new NotificationEventPathFilter(Pattern.compile(""), true).accept(null));
    }

    @Test
    public void acceptPathShouldProperlyIgnorePathsWhereTheLastComponentStartsWithADot() throws Exception {
        PathFilter filter = new NotificationEventPathFilter(Pattern.compile(".*"), true);
        assertFalse(filter.accept(new Path("/.some_hidden_file")));
        assertFalse(filter.accept(new Path("/some/long/path/.some_hidden_file/")));
    }

    @Test
    public void acceptPathShouldProperlyAcceptPathsWhereTheNonLastComponentStartsWithADot() throws Exception {
        PathFilter filter = new NotificationEventPathFilter(Pattern.compile(".*"), true);
        assertTrue(filter.accept(new Path("/some/long/path/.some_hidden_file/should/work")));
        assertTrue(filter.accept(new Path("/.some_hidden_file/should/still/accept")));
    }

    @Test
    public void acceptPathShouldProperlyMatchAllSubdirectoriesThatMatchWatchDirectoryAndFileFilter() throws Exception {
        PathFilter filter = new NotificationEventPathFilter(Pattern.compile("/root(/.*)?"), true);
        assertTrue(filter.accept(new Path("/root/sometest.txt")));
    }

    @Test
    public void acceptPathShouldProperlyMatchWhenWatchDirectoryMatchesPath() throws Exception {
        PathFilter filter = new NotificationEventPathFilter(Pattern.compile("/root(/.*)?"), false);
        assertTrue(filter.accept(new Path("/root")));
    }
}
