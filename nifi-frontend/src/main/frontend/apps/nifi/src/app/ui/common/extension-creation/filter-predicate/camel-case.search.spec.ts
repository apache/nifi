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

import { matchesCamelCaseSearch } from './camel-case.search';

describe('matchesCamelCaseSearch', () => {
    const value = 'org.apache.nifi.processors.standard.GenerateFlowFile';

    it.each([
        { value, query: '', expectedResult: false },
        { value, query: '[', expectedResult: false },
        { value, query: value, expectedResult: true },
        { value, query: 'Generate', expectedResult: true },
        { value, query: 'GFlowFile', expectedResult: true },
        { value, query: 'GeFlF', expectedResult: true },
        { value, query: 'GFF', expectedResult: true },
        { value, query: 'RFlowFile', expectedResult: false }
    ])('should return $expectedResult for matching $query against $value', ({ value, query, expectedResult }) => {
        expect(matchesCamelCaseSearch(value, query)).toBe(expectedResult);
    });
});
