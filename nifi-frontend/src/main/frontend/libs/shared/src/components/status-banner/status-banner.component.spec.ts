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
import { StatusVariant } from '../../types';
import { StatusBanner } from './status-banner.component';

describe('StatusBanner', () => {
    let component: StatusBanner;
    let fixture: ComponentFixture<StatusBanner>;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [StatusBanner]
        }).compileComponents();

        fixture = TestBed.createComponent(StatusBanner);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('defaults variant to critical', () => {
        expect(component.variant).toBe('critical');
    });

    it('should show the correct FA icon class for each variant', () => {
        const checkCircle: StatusVariant[] = ['success'];
        expect(checkCircle.every((variant) => component.getBannerIcon(variant) === 'fa-check-circle-o')).toBe(true);

        const infoCircle: StatusVariant[] = ['info', 'neutral'];
        expect(infoCircle.every((variant) => component.getBannerIcon(variant) === 'fa-info-circle')).toBe(true);

        const warning: StatusVariant[] = ['critical', 'caution', 'active'];
        expect(warning.every((variant) => component.getBannerIcon(variant) === 'fa-warning')).toBe(true);
    });

    it('should emit dismiss event when dismiss is clicked', () => {
        vi.spyOn(component.dismiss, 'next');
        component.dismissClicked();
        expect(component.dismiss.next).toHaveBeenCalled();
    });
});
