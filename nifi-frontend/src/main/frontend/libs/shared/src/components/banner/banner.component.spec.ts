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
import { Banner } from './banner.component';

describe('Banner', () => {
    let component: Banner;
    let fixture: ComponentFixture<Banner>;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [Banner]
        }).compileComponents();

        fixture = TestBed.createComponent(Banner);
        component = fixture.componentInstance;
    });

    it('should create', () => {
        fixture.detectChanges();
        expect(component).toBeTruthy();
    });

    it('should show single message', () => {
        fixture.componentRef.setInput('messages', ['Test message']);
        fixture.detectChanges();
        const messageElement = fixture.nativeElement.querySelector('div.error-banner-message');
        expect(messageElement).toBeTruthy();
        expect(messageElement.textContent).toContain('Test message');
    });

    it('should show multiple messages', () => {
        fixture.componentRef.setInput('messages', ['Test message 1', 'Test message 2']);
        fixture.detectChanges();
        const messageElements = fixture.nativeElement.querySelectorAll('li.error-banner-message');
        expect(messageElements.length).toBe(2);
        expect(messageElements[0].textContent).toContain('Test message 2');
        expect(messageElements[1].textContent).toContain('Test message 1');
    });

    it('should show deduplicated messages', () => {
        fixture.componentRef.setInput('messages', ['Test message 1', 'Test message 1', 'Test message 2']);
        fixture.detectChanges();
        const messageElements = fixture.nativeElement.querySelectorAll('li.error-banner-message');
        expect(messageElements.length).toBe(2);
        expect(messageElements[0].textContent).toContain('Test message 2');
        expect(messageElements[1].textContent).toContain('Test message 1');
    });
});
