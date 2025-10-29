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

import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BulletinsTip } from './bulletins-tip.component';

describe('Bulletins', () => {
    let component: BulletinsTip;
    let fixture: ComponentFixture<BulletinsTip>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [BulletinsTip]
        });
        fixture = TestBed.createComponent(BulletinsTip);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should return proper severity classes', () => {
        expect(component.getSeverity('error')).toBe('bulletin-error error-color');
        expect(component.getSeverity('warn')).toBe('bulletin-warn caution-color');
        expect(component.getSeverity('warning')).toBe('bulletin-warn caution-color');
        expect(component.getSeverity('info')).toBe('bulletin-normal success-color-default');
    });

    it('should compose bulletin copy message with optional stack trace', () => {
        const withStack = {
            bulletin: {
                message: 'm',
                stackTrace: 'st'
            }
        } as any;
        const withoutStack = {
            bulletin: {
                message: 'm'
            }
        } as any;

        expect(component.getBulletinCopyMessage(withStack)).toBe('m\n\nst');
        expect(component.getBulletinCopyMessage(withoutStack)).toBe('m');
    });
});
