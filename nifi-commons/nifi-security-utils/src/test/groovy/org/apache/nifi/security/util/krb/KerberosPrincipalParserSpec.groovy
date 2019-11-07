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
package org.apache.nifi.security.util.krb

import spock.lang.Specification
import spock.lang.Unroll

class KerberosPrincipalParserSpec extends Specification {

    @Unroll
    def "Verify parsed realm from '#testPrincipal' == '#expectedRealm'"() {
        expect:
        KerberosPrincipalParser.getRealm(testPrincipal) == expectedRealm

        where:
        testPrincipal                     || expectedRealm
        "user"                            || null
        "user@"                           || null
        "user@/instance"                  || "/instance"
        "user@EXAMPLE.COM"                || "EXAMPLE.COM"
        "user@name@EXAMPLE.COM"           || "EXAMPLE.COM"
        "user\\@"                         || null
        "user\\@name"                     || null
        "user\\@name@EXAMPLE.COM"         || "EXAMPLE.COM"
        "user@EXAMPLE.COM\\@"             || "EXAMPLE.COM\\@"
        "user@@name@\\@@\\@"              || "\\@"
        "user@@name@\\@@\\@@EXAMPLE.COM"  || "EXAMPLE.COM"
        "user@@name@\\@@\\@@EXAMPLE.COM@" || null
        "user\\@\\@name@EXAMPLE.COM"      || "EXAMPLE.COM"
    }

    @Unroll
    def "Verify parsed shortname from '#testPrincipal' == '#expectedShortname'"() {
        expect:
        KerberosPrincipalParser.getShortname(testPrincipal) == expectedShortname

        where:
        testPrincipal                              || expectedShortname
        "user"                                     || "user"
        "user/instance"                            || "user"
        "user@"                                    || "user@"
        "user@/instance"                           || "user"
        "user/instance@"                           || "user"
        "user@EXAMPLE.COM"                         || "user"
        "user@EXAMPLE.COM@"                        || "user@EXAMPLE.COM@"
        "user/instance@EXAMPLE.COM"                || "user"
        "user/instance/instance@EXAMPLE.COM"       || "user"
        "user\\/instance/instance@EXAMPLE.COM"     || "user\\/instance"
        "user/instance\\/instance@EXAMPLE.COM"     || "user"
        "user@name@EXAMPLE.COM"                    || "user@name"
        "user@/instance@EXAMPLE.COM"               || "user@"
        "user@name/instance@EXAMPLE.COM"           || "user@name"
        "user@/instance/instance@EXAMPLE.COM"      || "user@"
        "user@name/instance/instance@EXAMPLE.COM"  || "user@name"
        "user\\@"                                  || "user\\@"
        "user\\@/instance"                         || "user\\@"
        "user\\@/instance@EXAMPLE.COM"             || "user\\@"
        "user\\@name"                              || "user\\@name"
        "user\\@name/instance"                     || "user\\@name"
        "user\\@name/instance@EXAMPLE.COM"         || "user\\@name"
        "user@EXAMPLE.COM\\@"                      || "user"
        "user/instance@EXAMPLE.COM\\@"             || "user"
        "user@@name@\\@@\\@"                       || "user@@name@\\@"
        "user/instance@@name@\\@@\\@"              || "user"
        "user@@name@\\@@\\@"                       || "user@@name@\\@"
        "user@@/instance@\\@@\\@"                  || "user@@"
        "user@@name@\\@@\\@@EXAMPLE.COM"           || "user@@name@\\@@\\@"
        "user@@name@\\@@\\@/instance@EXAMPLE.COM"  || "user@@name@\\@@\\@"
        "user@@name@\\@@\\@@EXAMPLE.COM@"          || "user@@name@\\@@\\@@EXAMPLE.COM@"
        "user@@name@\\@@\\@/instance@EXAMPLE.COM@" || "user@@name@\\@@\\@"
        "user\\@\\@name@EXAMPLE.COM"               || "user\\@\\@name"
        "user\\@\\@name/instance@EXAMPLE.COM"      || "user\\@\\@name"
    }

    @Unroll
    def "Verify parsed instance from '#testPrincipal' == '#expectedInstance'"() {
        expect:
        KerberosPrincipalParser.getInstance(testPrincipal) == expectedInstance

        where:
        testPrincipal                              || expectedInstance
        "user"                                     || null
        "user@EXAMPLE.COM"                         || null
        "user/instance"                            || "instance"
        "user/instance@EXAMPLE.COM"                || "instance"
        "user@"                                    || null
        "user@/instance"                           || null
        "user/instance@"                           || "instance@"
        "user/instance/instance@EXAMPLE.COM"       || "instance/instance"
        "user\\/instance/instance@EXAMPLE.COM"     || "instance"
        "user/instance\\/instance@EXAMPLE.COM"     || "instance\\/instance"
        "user@name@EXAMPLE.COM"                    || null
        "user@/instance@EXAMPLE.COM"               || "instance"
        "user@name/instance@EXAMPLE.COM"           || "instance"
        "user@/instance/instance@EXAMPLE.COM"      || "instance/instance"
        "user@name/instance/instance@EXAMPLE.COM"  || "instance/instance"
        "user\\@"                                  || null
        "user\\@/instance"                         || "instance"
        "user\\@/instance@EXAMPLE.COM"             || "instance"
        "user\\@name"                              || null
        "user\\@name/instance"                     || "instance"
        "user\\@name/instance@EXAMPLE.COM"         || "instance"
        "user@EXAMPLE.COM\\@"                      || null
        "user/instance@EXAMPLE.COM\\@"             || "instance"
        "user@@name@\\@@\\@"                       || null
        "user/instance@@name@\\@@\\@"              || "instance@@name@\\@"
        "user@@name@\\@@\\@"                       || null
        "user@@/instance@\\@@\\@"                  || "instance@\\@"
        "user@@name@\\@@\\@@EXAMPLE.COM"           || null
        "user@@name@\\@@\\@/instance@EXAMPLE.COM"  || "instance"
        "user@@name@\\@@\\@@EXAMPLE.COM@"          || null
        "user@@name@\\@@\\@/instance@EXAMPLE.COM@" || "instance@EXAMPLE.COM@"
        "user\\@\\@name@EXAMPLE.COM"               || null
        "user\\@\\@name/instance@EXAMPLE.COM"      || "instance"
    }
}