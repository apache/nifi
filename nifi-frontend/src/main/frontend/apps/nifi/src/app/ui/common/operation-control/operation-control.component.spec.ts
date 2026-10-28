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
import { ComponentType, Storage } from '@nifi/shared';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { SelectionTarget } from '../canvas/utils/selection.utils';
import { CanvasOperationControl } from './operation-control.component';

const mockStorage = {
    getItem: vi.fn(),
    setItem: vi.fn()
};

function processor(id = 'processor-1', runStatus = 'Stopped'): SelectionTarget {
    return {
        entity: {
            id,
            permissions: { canRead: true, canWrite: true },
            revision: { version: 1 },
            status: { aggregateSnapshot: { runStatus, activeThreadCount: 0 } },
            component: {}
        },
        ui: { componentType: ComponentType.Processor }
    } as SelectionTarget;
}

async function setup(selection: SelectionTarget[] = [processor()], currentProcessGroupId: string | null = null) {
    await TestBed.configureTestingModule({
        imports: [CanvasOperationControl],
        providers: [{ provide: Storage, useValue: mockStorage }]
    }).compileComponents();
    const fixture = TestBed.createComponent(CanvasOperationControl);
    fixture.componentRef.setInput('selection', selection);
    fixture.componentRef.setInput('currentProcessGroupId', currentProcessGroupId);
    fixture.detectChanges();
    return { fixture, component: fixture.componentInstance };
}

describe('CanvasOperationControl', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockStorage.getItem.mockReturnValue(null);
    });

    it('restores the expanded state using the panel storage key', async () => {
        mockStorage.getItem.mockReturnValue({ 'operation-control': true });

        const { component } = await setup();

        expect(component.operationCollapsed).toBe(false);
    });

    it('persists expansion changes without replacing other panel preferences', async () => {
        mockStorage.getItem.mockReturnValue({ 'navigation-control': false });
        const { component } = await setup();

        component.toggleCollapsed(false);

        expect(mockStorage.setItem).toHaveBeenCalledWith('graph-control-visibility', {
            'navigation-control': false,
            'operation-control': true
        });
    });

    it('enables start and run once for a stopped processor', async () => {
        const { fixture } = await setup();

        expect(fixture.nativeElement.querySelector('[data-qa="operation-start"]').disabled).toBe(false);
        expect(fixture.nativeElement.querySelector('[data-qa="operation-run-once"]').disabled).toBe(false);
    });

    it('uses standard graph-control chrome and accessible button labels', async () => {
        const { fixture } = await setup();
        const buttons = Array.from(fixture.nativeElement.querySelectorAll('[data-qa^="operation-"]')) as HTMLElement[];

        expect(fixture.nativeElement.querySelector('mat-panel-title').textContent).toContain('Operation');
        expect(fixture.nativeElement.querySelector('mat-panel-title .fa-hand-o-up')).not.toBeNull();
        expect(buttons.map((button) => button.getAttribute('aria-label'))).toEqual([
            'Start',
            'Stop',
            'Enable',
            'Disable',
            'Run Once'
        ]);
        expect(buttons.every((button) => button.classList.contains('primary-icon-button'))).toBe(true);
    });

    it('emits the selection when start is clicked', async () => {
        const selection = [processor()];
        const { fixture, component } = await setup(selection);
        const listener = vi.fn();
        component.startComponents.subscribe(listener);

        fixture.nativeElement.querySelector('[data-qa="operation-start"]').click();

        expect(listener).toHaveBeenCalledWith(selection);
    });

    it('enables start when any available selected component is startable', async () => {
        const { component } = await setup([processor('stopped'), processor('running', 'Running')]);

        expect(component.canStart()).toBe(true);
    });

    it('keeps terminate threads out of the operation panel', async () => {
        const { fixture, component } = await setup();

        expect(fixture.nativeElement.querySelector('[data-qa="operation-terminate"]')).toBeNull();
        expect((component as unknown as Record<string, unknown>)['terminate']).toBeUndefined();
    });

    it('shows the selected component name, type, and id', async () => {
        const selected = processor();
        (selected.entity as { component: { name: string } }).component = { name: 'Route Text' };
        const { fixture, component } = await setup([selected]);

        expect(component.contextName()).toBe('Route Text');
        expect(component.contextType()).toBe(ComponentType.Processor);
        expect(component.contextId()).toBe('processor-1');
        expect(fixture.nativeElement.querySelector('component-context .context-name').textContent).toContain(
            'Route Text'
        );
        expect(fixture.nativeElement.querySelector('component-context .context-id').textContent).toContain(
            'processor-1'
        );
    });

    it('shows the current process group when nothing is selected', async () => {
        const { fixture, component } = await setup([], 'pg-1');
        fixture.componentRef.setInput('breadcrumbEntity', {
            id: 'pg-1',
            permissions: { canRead: true, canWrite: true },
            versionedFlowState: 'UP_TO_DATE',
            breadcrumb: { id: 'pg-1', name: 'Managed Flow' }
        });
        fixture.detectChanges();

        expect(component.contextName()).toBe('Managed Flow');
        expect(component.contextType()).toBe(ComponentType.ProcessGroup);
        expect(component.contextId()).toBe('pg-1');
        expect(fixture.nativeElement.querySelector('component-context .context-name').textContent).toContain(
            'Managed Flow'
        );
    });

    it('uses the current process group id when its breadcrumb is unavailable', async () => {
        const { component } = await setup([], 'pg-1');

        expect(component.contextName()).toBe('pg-1');
        expect(component.contextType()).toBe(ComponentType.ProcessGroup);
        expect(component.contextId()).toBe('pg-1');
    });

    it('shows a multiple-selection label without an id', async () => {
        const { fixture, component } = await setup([processor('one'), processor('two', 'Running')]);

        expect(component.contextName()).toBe('Multiple components selected');
        expect(component.contextType()).toBeNull();
        expect(component.contextId()).toBe('');
        expect(fixture.nativeElement.querySelector('component-context .context-id')).toBeNull();
    });
});
