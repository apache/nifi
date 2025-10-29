/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { BulletinBoardList } from './bulletin-board-list.component';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

describe('BulletinBoardList', () => {
    let component: BulletinBoardList;
    let fixture: ComponentFixture<BulletinBoardList>;

    beforeEach(() => {
        Element.prototype.scroll = jest.fn();
        TestBed.configureTestingModule({
            imports: [BulletinBoardList, NoopAnimationsModule, RouterTestingModule]
        });
        fixture = TestBed.createComponent(BulletinBoardList);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should emit filterChanged when applyFilter is called', () => {
        const received: any[] = [];
        component.filterChanged.subscribe((args) => received.push(args));
        component.applyFilter('foo', 'message');
        expect(received).toEqual([{ filterTerm: 'foo', filterColumn: 'message' }]);
    });

    it('should debounce filter term changes before emitting', fakeAsync(() => {
        const received: any[] = [];
        component.filterChanged.subscribe((args) => received.push(args));

        component.filterForm.get('filterTerm')!.setValue('abc');
        tick(499);
        expect(received.length).toBe(0);
        tick(1);
        expect(received).toEqual([{ filterTerm: 'abc', filterColumn: 'message' }]);
    }));

    it('should emit immediately when filter column changes', () => {
        const received: any[] = [];
        component.filterChanged.subscribe((args) => received.push(args));
        component.filterForm.get('filterColumn')!.setValue('name');
        expect(received).toEqual([{ filterTerm: '', filterColumn: 'name' }]);
    });

    it('should return proper severity classes', () => {
        expect(component.getSeverity('error')).toBe('bulletin-error error-color');
        expect(component.getSeverity('warn')).toBe('bulletin-warn caution-color');
        expect(component.getSeverity('warning')).toBe('bulletin-warn caution-color');
        expect(component.getSeverity('info')).toBe('bulletin-normal success-color-default');
    });

    it('should determine bulletin vs event types correctly', () => {
        const bulletin = {
            canRead: true,
            id: 1,
            sourceId: 's1',
            groupId: 'g1',
            timestamp: 'now',
            bulletin: {
                id: 1,
                sourceId: 's1',
                groupId: 'g1',
                category: 'cat',
                level: 'INFO',
                message: 'm',
                sourceName: 'sn',
                timestamp: 'now',
                sourceType: 'PROCESSOR'
            }
        } as any;

        const event = { type: 'filter', message: 'applied' } as any;

        const bulletinItem = { item: bulletin } as any;
        const eventItem = { item: event } as any;

        expect(component.isBulletin(bulletinItem)).toBe(true);
        expect(component.asBulletin(bulletinItem)).toBe(bulletin);
        expect(component.asBulletinEvent(bulletinItem)).toBeNull();

        expect(component.isBulletin(eventItem)).toBe(false);
        expect(component.asBulletin(eventItem)).toBeNull();
        expect(component.asBulletinEvent(eventItem)).toBe(event);
    });

    it('should build router links for known component types and contexts', () => {
        const base = {
            canRead: true,
            id: 1,
            sourceId: 'sid',
            groupId: 'gid',
            timestamp: 't'
        } as any;

        const generateBulletin = (sourceType: string, groupId: string | null = 'gid') =>
            ({
                ...base,
                groupId: groupId ?? (undefined as any),
                bulletin: {
                    id: 1,
                    sourceId: 'sid',
                    groupId: groupId ?? undefined,
                    category: 'c',
                    level: 'INFO',
                    message: 'm',
                    sourceName: 'sn',
                    timestamp: 't',
                    sourceType
                }
            }) as any;

        expect(component.getRouterLink(generateBulletin('CONTROLLER_SERVICE'))).toEqual([
            '/process-groups',
            'gid',
            'controller-services',
            'sid'
        ]);
        expect(component.getRouterLink(generateBulletin('CONTROLLER_SERVICE', null))).toEqual([
            '/settings',
            'management-controller-services',
            'sid'
        ]);
        expect(component.getRouterLink(generateBulletin('REPORTING_TASK'))).toEqual([
            '/settings',
            'reporting-tasks',
            'sid'
        ]);
        expect(component.getRouterLink(generateBulletin('FLOW_REGISTRY_CLIENT'))).toEqual([
            '/settings',
            'registry-clients',
            'sid'
        ]);
        expect(component.getRouterLink(generateBulletin('FLOW_ANALYSIS_RULE'))).toEqual([
            '/settings',
            'flow-analysis-rules',
            'sid'
        ]);
        expect(component.getRouterLink(generateBulletin('PARAMETER_PROVIDER'))).toEqual([
            '/settings',
            'parameter-providers',
            'sid'
        ]);
        expect(component.getRouterLink(generateBulletin('PROCESSOR'))).toEqual([
            '/process-groups',
            'gid',
            'Processor',
            'sid'
        ]);
        expect(component.getRouterLink(generateBulletin('UNKNOWN'))).toBeNull();
    });

    it('should toggle stack trace expansion by bulletin id', () => {
        const bulletin = {
            canRead: true,
            id: 1,
            sourceId: 's1',
            groupId: 'g1',
            timestamp: 'now',
            bulletin: {
                id: 42,
                sourceId: 's1',
                groupId: 'g1',
                category: 'cat',
                level: 'ERROR',
                message: 'm',
                stackTrace: 'st',
                sourceName: 'sn',
                timestamp: 'now',
                sourceType: 'PROCESSOR'
            }
        } as any;

        expect(component.isExpanded(bulletin)).toBe(false);
        component.toggleStackTrace(bulletin);
        expect(component.isExpanded(bulletin)).toBe(true);
        component.toggleStackTrace(bulletin);
        expect(component.isExpanded(bulletin)).toBe(false);
    });

    it('should compose bulletin copy message with optional stack trace', () => {
        const base = {
            canRead: true,
            id: 1,
            sourceId: 's1',
            groupId: 'g1',
            timestamp: 'now'
        } as any;
        const withStack = {
            ...base,
            bulletin: {
                id: 1,
                sourceId: 's1',
                groupId: 'g1',
                category: 'c',
                level: 'ERROR',
                message: 'm',
                stackTrace: 'st',
                sourceName: 'sn',
                timestamp: 'now',
                sourceType: 'PROCESSOR'
            }
        } as any;
        const withoutStack = {
            ...base,
            bulletin: {
                id: 1,
                sourceId: 's1',
                groupId: 'g1',
                category: 'c',
                level: 'ERROR',
                message: 'm',
                sourceName: 'sn',
                timestamp: 'now',
                sourceType: 'PROCESSOR'
            }
        } as any;

        expect(component.getBulletinCopyMessage(withStack)).toBe('m\n\nst');
        expect(component.getBulletinCopyMessage(withoutStack)).toBe('m');
    });

    it('should auto-scroll when bulletins change', fakeAsync(() => {
        const scrollSpy = jest.spyOn(Element.prototype as any, 'scroll');
        scrollSpy.mockClear();
        const bulletin = {
            canRead: true,
            id: 1,
            sourceId: 's1',
            groupId: 'g1',
            timestamp: 'now',
            bulletin: {
                id: 1,
                sourceId: 's1',
                groupId: 'g1',
                category: 'c',
                level: 'INFO',
                message: 'm',
                sourceName: 'sn',
                timestamp: 'now',
                sourceType: 'PROCESSOR'
            }
        } as any;

        component.bulletinBoardItems = [{ item: bulletin } as any];
        fixture.detectChanges();
        tick(11);
        expect(scrollSpy).toHaveBeenCalledTimes(1);
    }));
});
