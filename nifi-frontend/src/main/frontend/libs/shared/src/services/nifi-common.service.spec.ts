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

import { TestBed } from '@angular/core/testing';

import { NiFiCommon } from './nifi-common.service';

describe('Common', () => {
    let service: NiFiCommon;

    beforeEach(() => {
        TestBed.configureTestingModule({});
        service = TestBed.inject(NiFiCommon);
    });

    it('should be created', () => {
        expect(service).toBeTruthy();
    });

    describe('formatInteger', () => {
        describe('traditional formatting (default)', () => {
            it('should format small numbers with commas', () => {
                expect(service.formatInteger(0)).toBe('0');
                expect(service.formatInteger(1)).toBe('1');
                expect(service.formatInteger(123)).toBe('123');
                expect(service.formatInteger(1234)).toBe('1,234');
                expect(service.formatInteger(12345)).toBe('12,345');
                expect(service.formatInteger(99999)).toBe('99,999');
            });

            it('should format large numbers with commas when compact notation is disabled', () => {
                expect(service.formatInteger(100000)).toBe('100,000');
                expect(service.formatInteger(150000)).toBe('150,000');
                expect(service.formatInteger(1000000)).toBe('1,000,000');
                expect(service.formatInteger(1250000)).toBe('1,250,000');
                expect(service.formatInteger(1000000000)).toBe('1,000,000,000');
            });

            it('should handle negative numbers correctly with traditional formatting', () => {
                expect(service.formatInteger(-1234)).toBe('-1,234');
                expect(service.formatInteger(-99999)).toBe('-99,999');
                expect(service.formatInteger(-100000)).toBe('-100,000');
                expect(service.formatInteger(-1250000)).toBe('-1,250,000');
            });
        });

        describe('compact notation formatting', () => {
            it('should format small numbers with commas even when compact notation is enabled', () => {
                expect(service.formatInteger(0, true)).toBe('0');
                expect(service.formatInteger(1, true)).toBe('1');
                expect(service.formatInteger(123, true)).toBe('123');
                expect(service.formatInteger(1234, true)).toBe('1,234');
                expect(service.formatInteger(12345, true)).toBe('12,345');
                expect(service.formatInteger(99999, true)).toBe('99,999');
            });

            it('should format numbers >= 100,000 with compact notation when enabled', () => {
                expect(service.formatInteger(100000, true)).toBe('100K');
                expect(service.formatInteger(150000, true)).toBe('150K');
                expect(service.formatInteger(1000000, true)).toBe('1M');
                expect(service.formatInteger(1250000, true)).toBe('1.25M');
                expect(service.formatInteger(1000000000, true)).toBe('1B');
                expect(service.formatInteger(2750000000, true)).toBe('2.75B');
                expect(service.formatInteger(1000000000000, true)).toBe('1T');
            });

            it('should handle negative numbers correctly with compact notation', () => {
                expect(service.formatInteger(-1234, true)).toBe('-1,234');
                expect(service.formatInteger(-99999, true)).toBe('-99,999');
                expect(service.formatInteger(-100000, true)).toBe('-100K');
                expect(service.formatInteger(-1250000, true)).toBe('-1.25M');
            });

            it('should handle edge cases around the 100,000 threshold with compact notation', () => {
                expect(service.formatInteger(99999, true)).toBe('99,999');
                expect(service.formatInteger(100000, true)).toBe('100K');
                expect(service.formatInteger(100001, true)).toBe('100K');
            });

            it('should format very large numbers with appropriate suffixes', () => {
                expect(service.formatInteger(1234567890, true)).toBe('1.23B');
                expect(service.formatInteger(12345678901234, true)).toBe('12.35T');
            });
        });
    });
});
