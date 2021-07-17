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
package org.apache.nifi.registry.security.util.kerberos

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
}
