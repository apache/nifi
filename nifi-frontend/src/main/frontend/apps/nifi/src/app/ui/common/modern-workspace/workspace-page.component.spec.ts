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

import { Component, signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { provideMockStore } from '@ngrx/store/testing';
import { MockComponent } from 'ng-mocks';
import { WorkspacePageComponent } from './workspace-page.component';
import { WorkspacePreferencesService } from './workspace-preferences.service';
import { Navigation } from '../navigation/navigation.component';
import { ModernNavigationComponent } from './modern-navigation/modern-navigation.component';
import { selectBannerText } from '../../../state/banner-text/banner-text.selectors';

@Component({ imports: [WorkspacePageComponent], template: '<workspace-page><p>Projected route</p></workspace-page>' })
class Host {}

describe('Operational route workspace', () => {
    it('preserves route content and a single header instance while switching presentation', () => {
        const enabled = signal(false);
        TestBed.configureTestingModule({
            imports: [Host],
            providers: [
                provideMockStore({ selectors: [{ selector: selectBannerText, value: null }] }),
                { provide: WorkspacePreferencesService, useValue: { enabled } }
            ]
        });
        TestBed.overrideComponent(WorkspacePageComponent, {
            remove: { imports: [Navigation, ModernNavigationComponent] },
            add: { imports: [MockComponent(Navigation), MockComponent(ModernNavigationComponent)] }
        });
        const fixture = TestBed.createComponent(Host);
        fixture.detectChanges();
        const header = fixture.nativeElement.querySelector('navigation');
        expect(header).not.toBeNull();
        expect(fixture.nativeElement.querySelector('modern-navigation')).toBeNull();
        enabled.set(true);
        fixture.detectChanges();
        expect(fixture.nativeElement.querySelectorAll('navigation')).toHaveLength(1);
        expect(fixture.nativeElement.querySelector('navigation')).toBe(header);
        expect(fixture.nativeElement.querySelectorAll('modern-navigation')).toHaveLength(1);
        expect(fixture.nativeElement.textContent).toContain('Projected route');
        enabled.set(false);
        fixture.detectChanges();
        expect(fixture.nativeElement.querySelector('navigation')).toBe(header);
        expect(fixture.nativeElement.querySelector('modern-navigation')).toBeNull();
    });
});
