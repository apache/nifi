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

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Store } from '@ngrx/store';

import { DeprecationBannerComponent } from './deprecation-banner.component';

describe('DeprecationBannerComponent', () => {
    let fixture: ComponentFixture<DeprecationBannerComponent>;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [DeprecationBannerComponent],
            providers: [
                {
                    provide: Store,
                    useValue: {
                        select: vi.fn()
                    }
                }
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(DeprecationBannerComponent);
        fixture.detectChanges();
    });

    it('should display a custom deprecation reason', () => {
        fixture.componentRef.setInput('deprecationReason', 'This extension has been replaced.');
        fixture.detectChanges();

        const notice: HTMLElement = fixture.nativeElement.querySelector('.caution-color-background');

        expect(notice).toBeTruthy();
        expect(notice.textContent).toContain('Deprecation Notice');
        expect(notice.textContent).toContain('This extension has been replaced.');
    });

    it('should display the default message using the supplied fallback noun', () => {
        fixture.componentRef.setInput('fallbackNoun', 'connector');
        fixture.detectChanges();

        const notice: HTMLElement | null = fixture.nativeElement.querySelector('.caution-color-background');

        expect(notice).toBeTruthy();
        expect(notice?.textContent).toContain(
            'Please be aware this connector is deprecated and may be removed in the near future.'
        );
    });
});
