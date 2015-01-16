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
with (Scripting) {
    var a = new Relationship.Builder().name("a").description("some good stuff").build()
    var b = new Relationship.Builder().name("b").description("some other stuff").build()
    var c = new Relationship.Builder().name("c").description("some bad stuff").build()
    var instance = new ReaderScript({
        getExceptionRoute: function () {
            return c;
        },
        getRelationships: function () {
            return [a, b, c];
        },
        route: function (input) {
            var str = IOUtils.toString(input);
            var lines = str.split("\n");
            for (var line in lines) {
                if (lines[line].match(/^bad/i)) {
                    return b;
                } else if (lines[line].match(/^sed/i)) {
                    throw "That's no good!";
                }
            }
            return a;
        }
    });
}
