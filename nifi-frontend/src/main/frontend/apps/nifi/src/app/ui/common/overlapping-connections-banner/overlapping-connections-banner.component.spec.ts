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
import { OverlappingConnectionsBannerComponent } from './overlapping-connections-banner.component';
import { OverlappingConnectionGroup } from '../overlap-detection.utils';

interface SetupOptions {
    overlappingGroups?: OverlappingConnectionGroup[];
}

async function setup(options: SetupOptions = {}) {
    await TestBed.configureTestingModule({
        imports: [OverlappingConnectionsBannerComponent]
    }).compileComponents();

    const fixture: ComponentFixture<OverlappingConnectionsBannerComponent> = TestBed.createComponent(
        OverlappingConnectionsBannerComponent
    );
    const component = fixture.componentInstance;

    if (options.overlappingGroups) {
        fixture.componentRef.setInput('overlappingGroups', options.overlappingGroups);
    }

    fixture.detectChanges();

    return { fixture, component };
}

describe('OverlappingConnectionsBannerComponent', () => {
    beforeEach(() => {
        jest.clearAllMocks();
    });

    describe('rendering', () => {
        it('should not render when there are no overlapping groups', async () => {
            const { fixture } = await setup({ overlappingGroups: [] });
            const banner = fixture.nativeElement.querySelector('.overlapping-connections-banner');
            expect(banner).toBeNull();
        });

        it('should render when there are overlapping groups', async () => {
            const groups: OverlappingConnectionGroup[] = [
                { sourceComponentId: 'a', destinationComponentId: 'b', connectionIds: ['c1', 'c2'] }
            ];
            const { fixture } = await setup({ overlappingGroups: groups });
            const banner = fixture.nativeElement.querySelector('.overlapping-connections-banner');
            expect(banner).toBeTruthy();
        });

        it('should display the correct count of overlap groups', async () => {
            const groups: OverlappingConnectionGroup[] = [
                { sourceComponentId: 'a', destinationComponentId: 'b', connectionIds: ['c1', 'c2'] },
                { sourceComponentId: 'c', destinationComponentId: 'd', connectionIds: ['c3', 'c4'] }
            ];
            const { fixture } = await setup({ overlappingGroups: groups });
            const banner = fixture.nativeElement.querySelector('.overlapping-connections-banner');
            expect(banner.textContent).toContain('2 overlapping connection group(s) detected');
        });

        it('should render a link for each overlap group', async () => {
            const groups: OverlappingConnectionGroup[] = [
                { sourceComponentId: 'a', destinationComponentId: 'b', connectionIds: ['c1', 'c2'] },
                { sourceComponentId: 'c', destinationComponentId: 'd', connectionIds: ['c3', 'c4', 'c5'] }
            ];
            const { fixture } = await setup({ overlappingGroups: groups });
            const links = fixture.nativeElement.querySelectorAll('.overlap-link');
            expect(links.length).toBe(2);
            expect(links[0].textContent).toContain('Overlap 1 (2 connections)');
            expect(links[1].textContent).toContain('Overlap 2 (3 connections)');
        });
    });

    describe('interaction', () => {
        it('should emit navigateToGroup when a link is clicked', async () => {
            const groups: OverlappingConnectionGroup[] = [
                { sourceComponentId: 'a', destinationComponentId: 'b', connectionIds: ['c1', 'c2'] }
            ];
            const { fixture, component } = await setup({ overlappingGroups: groups });

            const emitSpy = jest.spyOn(component.navigateToGroup, 'emit');
            const link = fixture.nativeElement.querySelector('.overlap-link');
            link.click();

            expect(emitSpy).toHaveBeenCalledWith(groups[0]);
        });
    });
});
