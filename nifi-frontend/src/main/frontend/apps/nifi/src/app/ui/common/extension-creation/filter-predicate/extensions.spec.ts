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

import { FilterPredicate } from './filter-predicate';
import { anyOf, onLowerCaseFilter } from './extensions';

describe('FilterPredicate extensions', () => {
    describe('onLowerCaseFilter', () => {
        let memoizedFilter: string | null = null;

        const mnemonicTautology: FilterPredicate<any> = (_, filter) => {
            memoizedFilter = filter;
            return true;
        };

        it('should create function that is passed lowercase filter values as is', () => {
            const lowerCaseInput = 'hello';

            const predicate = onLowerCaseFilter(mnemonicTautology);
            predicate('example', lowerCaseInput);

            expect(memoizedFilter).toEqual(lowerCaseInput);
        });

        it('should create function that is passed non-lowercase filter values as lowercase values', () => {
            const nonLowerCaseInput = 'HelLO WoRlD';
            const expectedPassedValue = 'hello world';

            const predicate = onLowerCaseFilter(mnemonicTautology);
            predicate('example', nonLowerCaseInput);

            expect(memoizedFilter).toEqual(expectedPassedValue);
        });
    });

    describe('anyOf', () => {
        const tautology: FilterPredicate<any> = () => true;
        const contradiction: FilterPredicate<any> = () => false;

        it('should create a function that yields true, when any of the functions passed to it yield true', () => {
            const predicate = anyOf(contradiction, contradiction, tautology, contradiction);
            const result = predicate('data', 'filter');

            expect(result).toEqual(true);
        });

        it('should create a function that yields false, when all of the functions passed to it yield false', () => {
            const predicate = anyOf(contradiction, contradiction, contradiction, contradiction, contradiction);
            const result = predicate('data', 'filter');

            expect(result).toEqual(false);
        });

        it('should create a function that yields false, when no function is passed to it', () => {
            const predicate = anyOf();
            const result = predicate('data', 'filter');

            expect(result).toEqual(false);
        });
    });
});
