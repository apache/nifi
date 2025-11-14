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

    describe('time conversion constants', () => {
        it('should have correct millisecond conversion constants', () => {
            expect(NiFiCommon.MILLIS_PER_SECOND).toBe(1000);
            expect(NiFiCommon.MILLIS_PER_MINUTE).toBe(60000);
            expect(NiFiCommon.MILLIS_PER_HOUR).toBe(3600000);
            expect(NiFiCommon.MILLIS_PER_DAY).toBe(86400000);
        });

        it('should have correct nanosecond conversion constant', () => {
            expect(NiFiCommon.NANOS_PER_MILLI).toBe(1000000);
        });

        it('should correctly convert nanoseconds to milliseconds', () => {
            const nanoseconds = 5000000000; // 5 seconds in nanos
            const milliseconds = nanoseconds / NiFiCommon.NANOS_PER_MILLI;
            expect(milliseconds).toBe(5000); // 5 seconds in millis
        });
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

    describe('getMostSevereBulletin', () => {
        it('should return null when no bulletins provided', () => {
            const result = service.getMostSevereBulletin([]);
            expect(result).toBeNull();
        });

        it('should return null when bulletins is null', () => {
            const result = service.getMostSevereBulletin(null as any);
            expect(result).toBeNull();
        });

        it('should return null when bulletins is undefined', () => {
            const result = service.getMostSevereBulletin(undefined as any);
            expect(result).toBeNull();
        });

        it('should return the bulletin when only one bulletin provided', () => {
            const bulletins = [
                {
                    id: 1,
                    canRead: true,
                    sourceId: 'source1',
                    groupId: 'group1',
                    timestamp: '2023-01-01T00:00:00Z',
                    timestampIso: '2023-01-01T00:00:00Z',
                    bulletin: {
                        id: 1,
                        sourceId: 'source1',
                        groupId: 'group1',
                        category: 'test',
                        sourceName: 'Source 1',
                        sourceType: 'PROCESSOR',
                        level: 'ERROR',
                        message: 'Error message',
                        timestamp: '2023-01-01T00:00:00Z',
                        timestampIso: '2023-01-01T00:00:00Z'
                    }
                }
            ];
            const result = service.getMostSevereBulletin(bulletins);
            expect(result).toEqual(bulletins[0]);
        });

        it('should return ERROR bulletin when multiple severities present', () => {
            const bulletins = [
                {
                    id: 1,
                    canRead: true,
                    sourceId: 'source1',
                    groupId: 'group1',
                    timestamp: '2023-01-01T00:00:00Z',
                    timestampIso: '2023-01-01T00:00:00Z',
                    bulletin: {
                        id: 1,
                        sourceId: 'source1',
                        groupId: 'group1',
                        category: 'test',
                        sourceName: 'Source 1',
                        sourceType: 'PROCESSOR',
                        level: 'INFO',
                        message: 'Info message',
                        timestamp: '2023-01-01T00:00:00Z',
                        timestampIso: '2023-01-01T00:00:00Z'
                    }
                },
                {
                    id: 2,
                    canRead: true,
                    sourceId: 'source2',
                    groupId: 'group2',
                    timestamp: '2023-01-01T00:00:00Z',
                    timestampIso: '2023-01-01T00:00:00Z',
                    bulletin: {
                        id: 2,
                        sourceId: 'source2',
                        groupId: 'group2',
                        category: 'test',
                        sourceName: 'Source 2',
                        sourceType: 'PROCESSOR',
                        level: 'ERROR',
                        message: 'Error message',
                        timestamp: '2023-01-01T00:00:00Z',
                        timestampIso: '2023-01-01T00:00:00Z'
                    }
                }
            ];
            const result = service.getMostSevereBulletin(bulletins);
            expect(result).toEqual(bulletins[1]);
        });

        it('should return WARNING bulletin when no ERROR present', () => {
            const bulletins = [
                {
                    id: 1,
                    canRead: true,
                    sourceId: 'source1',
                    groupId: 'group1',
                    timestamp: '2023-01-01T00:00:00Z',
                    timestampIso: '2023-01-01T00:00:00Z',
                    bulletin: {
                        id: 1,
                        sourceId: 'source1',
                        groupId: 'group1',
                        category: 'test',
                        sourceName: 'Source 1',
                        sourceType: 'PROCESSOR',
                        level: 'INFO',
                        message: 'Info message',
                        timestamp: '2023-01-01T00:00:00Z',
                        timestampIso: '2023-01-01T00:00:00Z'
                    }
                },
                {
                    id: 2,
                    canRead: true,
                    sourceId: 'source2',
                    groupId: 'group2',
                    timestamp: '2023-01-01T00:00:00Z',
                    timestampIso: '2023-01-01T00:00:00Z',
                    bulletin: {
                        id: 2,
                        sourceId: 'source2',
                        groupId: 'group2',
                        category: 'test',
                        sourceName: 'Source 2',
                        sourceType: 'PROCESSOR',
                        level: 'WARNING',
                        message: 'Warning message',
                        timestamp: '2023-01-01T00:00:00Z',
                        timestampIso: '2023-01-01T00:00:00Z'
                    }
                }
            ];
            const result = service.getMostSevereBulletin(bulletins);
            expect(result).toEqual(bulletins[1]);
        });
    });

    describe('getBulletinSeverityClass', () => {
        it('should return tertiary-color when no bulletins provided', () => {
            const result = service.getBulletinSeverityClass([]);
            expect(result).toBe('tertiary-color');
        });

        it('should return tertiary-color when bulletins is null', () => {
            const result = service.getBulletinSeverityClass(null as any);
            expect(result).toBe('tertiary-color');
        });

        it('should return error-color for error level bulletins', () => {
            const bulletins = [
                {
                    id: 1,
                    canRead: true,
                    sourceId: 'source1',
                    groupId: 'group1',
                    timestamp: '2023-01-01T00:00:00Z',
                    timestampIso: '2023-01-01T00:00:00Z',
                    bulletin: {
                        id: 1,
                        sourceId: 'source1',
                        groupId: 'group1',
                        category: 'test',
                        sourceName: 'Source 1',
                        sourceType: 'PROCESSOR',
                        level: 'ERROR',
                        message: 'Error message',
                        timestamp: '2023-01-01T00:00:00Z',
                        timestampIso: '2023-01-01T00:00:00Z'
                    }
                }
            ];
            const result = service.getBulletinSeverityClass(bulletins);
            expect(result).toBe('error-color');
        });

        it('should return caution-color for warning level bulletins', () => {
            const bulletins = [
                {
                    id: 1,
                    canRead: true,
                    sourceId: 'source1',
                    groupId: 'group1',
                    timestamp: '2023-01-01T00:00:00Z',
                    timestampIso: '2023-01-01T00:00:00Z',
                    bulletin: {
                        id: 1,
                        sourceId: 'source1',
                        groupId: 'group1',
                        category: 'test',
                        sourceName: 'Source 1',
                        sourceType: 'PROCESSOR',
                        level: 'WARNING',
                        message: 'Warning message',
                        timestamp: '2023-01-01T00:00:00Z',
                        timestampIso: '2023-01-01T00:00:00Z'
                    }
                }
            ];
            const result = service.getBulletinSeverityClass(bulletins);
            expect(result).toBe('caution-color');
        });

        it('should return caution-color for warn level bulletins (case insensitive)', () => {
            const bulletins = [
                {
                    id: 1,
                    canRead: true,
                    sourceId: 'source1',
                    groupId: 'group1',
                    timestamp: '2023-01-01T00:00:00Z',
                    timestampIso: '2023-01-01T00:00:00Z',
                    bulletin: {
                        id: 1,
                        sourceId: 'source1',
                        groupId: 'group1',
                        category: 'test',
                        sourceName: 'Source 1',
                        sourceType: 'PROCESSOR',
                        level: 'warn',
                        message: 'Warning message',
                        timestamp: '2023-01-01T00:00:00Z',
                        timestampIso: '2023-01-01T00:00:00Z'
                    }
                }
            ];
            const result = service.getBulletinSeverityClass(bulletins);
            expect(result).toBe('caution-color');
        });

        it('should return success-color-default for info level bulletins', () => {
            const bulletins = [
                {
                    id: 1,
                    canRead: true,
                    sourceId: 'source1',
                    groupId: 'group1',
                    timestamp: '2023-01-01T00:00:00Z',
                    timestampIso: '2023-01-01T00:00:00Z',
                    bulletin: {
                        id: 1,
                        sourceId: 'source1',
                        groupId: 'group1',
                        category: 'test',
                        sourceName: 'Source 1',
                        sourceType: 'PROCESSOR',
                        level: 'INFO',
                        message: 'Info message',
                        timestamp: '2023-01-01T00:00:00Z',
                        timestampIso: '2023-01-01T00:00:00Z'
                    }
                }
            ];
            const result = service.getBulletinSeverityClass(bulletins);
            expect(result).toBe('success-color-default');
        });

        it('should return success-color-default for debug level bulletins', () => {
            const bulletins = [
                {
                    id: 1,
                    canRead: true,
                    sourceId: 'source1',
                    groupId: 'group1',
                    timestamp: '2023-01-01T00:00:00Z',
                    timestampIso: '2023-01-01T00:00:00Z',
                    bulletin: {
                        id: 1,
                        sourceId: 'source1',
                        groupId: 'group1',
                        category: 'test',
                        sourceName: 'Source 1',
                        sourceType: 'PROCESSOR',
                        level: 'DEBUG',
                        message: 'Debug message',
                        timestamp: '2023-01-01T00:00:00Z',
                        timestampIso: '2023-01-01T00:00:00Z'
                    }
                }
            ];
            const result = service.getBulletinSeverityClass(bulletins);
            expect(result).toBe('success-color-default');
        });

        it('should return success-color-default for trace level bulletins', () => {
            const bulletins = [
                {
                    id: 1,
                    canRead: true,
                    sourceId: 'source1',
                    groupId: 'group1',
                    timestamp: '2023-01-01T00:00:00Z',
                    timestampIso: '2023-01-01T00:00:00Z',
                    bulletin: {
                        id: 1,
                        sourceId: 'source1',
                        groupId: 'group1',
                        category: 'test',
                        sourceName: 'Source 1',
                        sourceType: 'PROCESSOR',
                        level: 'TRACE',
                        message: 'Trace message',
                        timestamp: '2023-01-01T00:00:00Z',
                        timestampIso: '2023-01-01T00:00:00Z'
                    }
                }
            ];
            const result = service.getBulletinSeverityClass(bulletins);
            expect(result).toBe('success-color-default');
        });

        it('should return success-color-default for unknown level bulletins', () => {
            const bulletins = [
                {
                    id: 1,
                    canRead: true,
                    sourceId: 'source1',
                    groupId: 'group1',
                    timestamp: '2023-01-01T00:00:00Z',
                    timestampIso: '2023-01-01T00:00:00Z',
                    bulletin: {
                        id: 1,
                        sourceId: 'source1',
                        groupId: 'group1',
                        category: 'test',
                        sourceName: 'Source 1',
                        sourceType: 'PROCESSOR',
                        level: 'UNKNOWN',
                        message: 'Unknown message',
                        timestamp: '2023-01-01T00:00:00Z',
                        timestampIso: '2023-01-01T00:00:00Z'
                    }
                }
            ];
            const result = service.getBulletinSeverityClass(bulletins);
            expect(result).toBe('success-color-default');
        });

        it('should return error-color when multiple bulletins with different severities', () => {
            const bulletins = [
                {
                    id: 1,
                    canRead: true,
                    sourceId: 'source1',
                    groupId: 'group1',
                    timestamp: '2023-01-01T00:00:00Z',
                    timestampIso: '2023-01-01T00:00:00Z',
                    bulletin: {
                        id: 1,
                        sourceId: 'source1',
                        groupId: 'group1',
                        category: 'test',
                        sourceName: 'Source 1',
                        sourceType: 'PROCESSOR',
                        level: 'INFO',
                        message: 'Info message',
                        timestamp: '2023-01-01T00:00:00Z',
                        timestampIso: '2023-01-01T00:00:00Z'
                    }
                },
                {
                    id: 2,
                    canRead: true,
                    sourceId: 'source2',
                    groupId: 'group2',
                    timestamp: '2023-01-01T00:00:00Z',
                    timestampIso: '2023-01-01T00:00:00Z',
                    bulletin: {
                        id: 2,
                        sourceId: 'source2',
                        groupId: 'group2',
                        category: 'test',
                        sourceName: 'Source 2',
                        sourceType: 'PROCESSOR',
                        level: 'ERROR',
                        message: 'Error message',
                        timestamp: '2023-01-01T00:00:00Z',
                        timestampIso: '2023-01-01T00:00:00Z'
                    }
                }
            ];
            const result = service.getBulletinSeverityClass(bulletins);
            expect(result).toBe('error-color');
        });

        it('should return caution-color when multiple bulletins with warning being most severe', () => {
            const bulletins = [
                {
                    id: 1,
                    canRead: true,
                    sourceId: 'source1',
                    groupId: 'group1',
                    timestamp: '2023-01-01T00:00:00Z',
                    timestampIso: '2023-01-01T00:00:00Z',
                    bulletin: {
                        id: 1,
                        sourceId: 'source1',
                        groupId: 'group1',
                        category: 'test',
                        sourceName: 'Source 1',
                        sourceType: 'PROCESSOR',
                        level: 'INFO',
                        message: 'Info message',
                        timestamp: '2023-01-01T00:00:00Z',
                        timestampIso: '2023-01-01T00:00:00Z'
                    }
                },
                {
                    id: 2,
                    canRead: true,
                    sourceId: 'source2',
                    groupId: 'group2',
                    timestamp: '2023-01-01T00:00:00Z',
                    timestampIso: '2023-01-01T00:00:00Z',
                    bulletin: {
                        id: 2,
                        sourceId: 'source2',
                        groupId: 'group2',
                        category: 'test',
                        sourceName: 'Source 2',
                        sourceType: 'PROCESSOR',
                        level: 'WARNING',
                        message: 'Warning message',
                        timestamp: '2023-01-01T00:00:00Z',
                        timestampIso: '2023-01-01T00:00:00Z'
                    }
                }
            ];
            const result = service.getBulletinSeverityClass(bulletins);
            expect(result).toBe('caution-color');
        });
    });
});
