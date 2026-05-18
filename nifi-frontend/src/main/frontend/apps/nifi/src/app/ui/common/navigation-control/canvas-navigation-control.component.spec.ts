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
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { MockComponent } from 'ng-mocks';
import { Storage } from '@nifi/shared';
import { CanvasNavigationControl } from './canvas-navigation-control.component';
import { CanvasBirdseyeComponent } from '../birdseye/birdseye.component';

const mockStorage = {
    getItem: vi.fn().mockReturnValue(null),
    setItem: vi.fn(),
    removeItem: vi.fn(),
    hasItem: vi.fn(),
    getItemExpiration: vi.fn()
};

interface SetupOptions {
    canNavigateToParent?: boolean;
    storageKey?: string;
    storedVisibility?: { [key: string]: boolean } | null;
}

async function setup(options: SetupOptions = {}) {
    mockStorage.getItem.mockReturnValue(options.storedVisibility ?? null);

    await TestBed.configureTestingModule({
        imports: [CanvasNavigationControl, MockComponent(CanvasBirdseyeComponent), NoopAnimationsModule],
        providers: [{ provide: Storage, useValue: mockStorage }]
    }).compileComponents();

    const fixture: ComponentFixture<CanvasNavigationControl> = TestBed.createComponent(CanvasNavigationControl);
    const component = fixture.componentInstance;

    fixture.componentRef.setInput('birdseyeComponents', []);
    fixture.componentRef.setInput('birdseyeTransform', { translate: { x: 0, y: 0 }, scale: 1 });
    fixture.componentRef.setInput('canvasDimensions', { width: 800, height: 600 });
    fixture.componentRef.setInput('canNavigateToParent', options.canNavigateToParent ?? false);

    if (options.storageKey) {
        fixture.componentRef.setInput('storageKey', options.storageKey);
    }

    fixture.detectChanges();

    return { fixture, component };
}

describe('CanvasNavigationControl', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('Component initialization', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });

        it('should default to expanded state', async () => {
            const { component } = await setup();
            expect(component.navigationCollapsed).toBe(false);
        });

        it('should restore collapsed state from storage', async () => {
            const { component } = await setup({
                storedVisibility: { 'canvas-navigation-control': false }
            });
            expect(component.navigationCollapsed).toBe(true);
        });
    });

    describe('Zoom buttons', () => {
        it('should render the zoom in button', async () => {
            const { fixture } = await setup();
            expect(fixture.nativeElement.querySelector('[data-qa="zoom-in-button"]')).toBeTruthy();
        });

        it('should render the zoom out button', async () => {
            const { fixture } = await setup();
            expect(fixture.nativeElement.querySelector('[data-qa="zoom-out-button"]')).toBeTruthy();
        });

        it('should render the zoom fit button', async () => {
            const { fixture } = await setup();
            expect(fixture.nativeElement.querySelector('[data-qa="zoom-fit-button"]')).toBeTruthy();
        });

        it('should render the zoom actual button', async () => {
            const { fixture } = await setup();
            expect(fixture.nativeElement.querySelector('[data-qa="zoom-actual-button"]')).toBeTruthy();
        });

        it('should emit zoomIn when the zoom in button is clicked', async () => {
            const { fixture, component } = await setup();
            const emitSpy = vi.spyOn(component.zoomIn, 'emit');

            fixture.nativeElement.querySelector('[data-qa="zoom-in-button"]').click();

            expect(emitSpy).toHaveBeenCalledTimes(1);
        });

        it('should emit zoomOut when the zoom out button is clicked', async () => {
            const { fixture, component } = await setup();
            const emitSpy = vi.spyOn(component.zoomOut, 'emit');

            fixture.nativeElement.querySelector('[data-qa="zoom-out-button"]').click();

            expect(emitSpy).toHaveBeenCalledTimes(1);
        });

        it('should emit zoomFit when the zoom fit button is clicked', async () => {
            const { fixture, component } = await setup();
            const emitSpy = vi.spyOn(component.zoomFit, 'emit');

            fixture.nativeElement.querySelector('[data-qa="zoom-fit-button"]').click();

            expect(emitSpy).toHaveBeenCalledTimes(1);
        });

        it('should emit zoomActual when the zoom actual button is clicked', async () => {
            const { fixture, component } = await setup();
            const emitSpy = vi.spyOn(component.zoomActual, 'emit');

            fixture.nativeElement.querySelector('[data-qa="zoom-actual-button"]').click();

            expect(emitSpy).toHaveBeenCalledTimes(1);
        });
    });

    describe('Leave group button', () => {
        it('should be hidden when canNavigateToParent is false', async () => {
            const { fixture } = await setup({ canNavigateToParent: false });
            expect(fixture.nativeElement.querySelector('[data-qa="leave-group-button"]')).toBeNull();
        });

        it('should be shown when canNavigateToParent is true', async () => {
            const { fixture } = await setup({ canNavigateToParent: true });
            expect(fixture.nativeElement.querySelector('[data-qa="leave-group-button"]')).toBeTruthy();
        });

        it('should emit leaveGroup when clicked', async () => {
            const { fixture, component } = await setup({ canNavigateToParent: true });
            const emitSpy = vi.spyOn(component.leaveGroup, 'emit');

            fixture.nativeElement.querySelector('[data-qa="leave-group-button"]').click();

            expect(emitSpy).toHaveBeenCalledTimes(1);
        });
    });

    describe('Birdseye embedding', () => {
        it('should render the embedded birdseye component', async () => {
            const { fixture } = await setup();
            expect(fixture.nativeElement.querySelector('canvas-birdseye')).toBeTruthy();
        });
    });

    describe('Collapse persistence', () => {
        it('should save collapsed state under the default key', async () => {
            const { component } = await setup();

            component.toggleCollapsed(true);

            expect(mockStorage.setItem).toHaveBeenCalledWith('graph-control-visibility', {
                'canvas-navigation-control': false
            });
        });

        it('should save expanded state under the default key', async () => {
            const { component } = await setup();

            component.toggleCollapsed(false);

            expect(mockStorage.setItem).toHaveBeenCalledWith('graph-control-visibility', {
                'canvas-navigation-control': true
            });
        });

        it('should use a custom storageKey for persistence', async () => {
            const { component } = await setup({ storageKey: 'connector-navigation-control' });

            component.toggleCollapsed(true);

            expect(mockStorage.setItem).toHaveBeenCalledWith('graph-control-visibility', {
                'connector-navigation-control': false
            });
        });

        it('should restore collapsed state using a custom storageKey', async () => {
            const { component } = await setup({
                storageKey: 'connector-navigation-control',
                storedVisibility: { 'connector-navigation-control': false }
            });

            expect(component.navigationCollapsed).toBe(true);
        });

        it('should not cross-pollinate state between different storage keys', async () => {
            const { component } = await setup({
                storageKey: 'connector-navigation-control',
                storedVisibility: { 'navigation-control': false }
            });

            expect(component.navigationCollapsed).toBe(false);
        });

        it('should preserve other entries when persisting its key', async () => {
            const { component } = await setup({
                storageKey: 'connector-navigation-control',
                storedVisibility: { 'navigation-control': true }
            });

            component.toggleCollapsed(true);

            expect(mockStorage.setItem).toHaveBeenCalledWith('graph-control-visibility', {
                'navigation-control': true,
                'connector-navigation-control': false
            });
        });
    });
});
