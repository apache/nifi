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
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import type { Mock } from 'vitest';

import { CanvasContextMenu } from './canvas-context-menu.service';
import { CanvasUtils } from './canvas-utils.service';
import { Client } from '../../../service/client.service';
import { CanvasView } from './canvas-view.service';
import { CanvasActionsService } from './canvas-actions.service';
import { DraggableBehavior } from './behavior/draggable-behavior.service';
import { promptDrain, viewDrain } from '../state/process-group-drain/process-group-drain.actions';
import type { ResolvedExecutionEngine } from '../state/flow';
import type { ContextMenuItemDefinition } from '../../../ui/common/context-menu/context-menu.component';

interface SetupOptions {
    currentProcessGroupId?: string;
    isProcessGroup?: boolean;
    resolvedExecutionEngine?: ResolvedExecutionEngine;
    canRead?: boolean;
    canModify?: boolean;
    canReadCurrentProcessGroup?: boolean;
    canModifyCurrentProcessGroup?: boolean;
    currentDrainRequestId?: string | null;
}

function menuItem(menuItems: ContextMenuItemDefinition[], text: string): ContextMenuItemDefinition {
    const item = menuItems.find((candidate) => candidate.text === text);
    if (!item) {
        throw new Error(`Expected menu item "${text}"`);
    }
    return item;
}

async function setup(options: SetupOptions = {}) {
    const currentProcessGroupId = options.currentProcessGroupId ?? 'current-pg';
    const canvasUtils = {
        getProcessGroupId: vi.fn().mockReturnValue(currentProcessGroupId),
        isProcessGroup: vi.fn().mockReturnValue(options.isProcessGroup ?? false),
        getResolvedExecutionEngine: vi.fn().mockReturnValue(options.resolvedExecutionEngine ?? 'STANDARD'),
        canRead: vi.fn().mockReturnValue(options.canRead ?? true),
        canModify: vi.fn().mockReturnValue(options.canModify ?? true),
        canReadCurrentProcessGroup: vi.fn().mockReturnValue(options.canReadCurrentProcessGroup ?? true),
        canModifyCurrentProcessGroup: vi.fn().mockReturnValue(options.canModifyCurrentProcessGroup ?? true),
        getCurrentProcessGroupDrainRequestId: vi.fn().mockReturnValue(options.currentDrainRequestId ?? null)
    };

    await TestBed.configureTestingModule({
        providers: [
            CanvasContextMenu,
            provideMockStore(),
            { provide: CanvasUtils, useValue: canvasUtils },
            { provide: Client, useValue: {} },
            { provide: CanvasView, useValue: {} },
            {
                provide: CanvasActionsService,
                useValue: {
                    getConditionFunction: () => () => false,
                    getActionFunction: () => () => undefined
                }
            },
            { provide: DraggableBehavior, useValue: {} }
        ]
    }).compileComponents();

    const service = TestBed.inject(CanvasContextMenu);
    const store = TestBed.inject(MockStore);
    const dispatchSpy = vi.spyOn(store, 'dispatch') as Mock;

    return { service, canvasUtils, dispatchSpy };
}

describe('CanvasContextMenu', () => {
    describe('Drain', () => {
        it('is immediately after Stop in the root menu', async () => {
            const { service } = await setup();
            const menuItems = service.getMenu('root')!.menuItems;
            const texts = menuItems.map((item) => item.text);

            expect(texts.indexOf('Stop')).toBeGreaterThan(-1);
            expect(texts).not.toContain('Stop Sources');
            expect(texts.indexOf('Drain')).toBe(texts.indexOf('Stop') + 1);
            expect(texts.indexOf('View Drain')).toBe(texts.indexOf('Drain') + 1);
        });

        it('is visible on an empty canvas and dispatches promptDrain for the current group', async () => {
            const { service, dispatchSpy } = await setup({ currentProcessGroupId: 'current-pg' });
            const drain = menuItem(service.getMenu('root')!.menuItems, 'Drain');
            const selection = { empty: () => true };

            expect(drain.condition!(selection as never)).toBe(true);
            drain.action!(selection as never);

            expect(dispatchSpy).toHaveBeenCalledWith(promptDrain({ request: { processGroupId: 'current-pg' } }));
        });

        it('is visible for a selected process group and dispatches promptDrain for that group', async () => {
            const { service, dispatchSpy } = await setup({ isProcessGroup: true });
            const drain = menuItem(service.getMenu('root')!.menuItems, 'Drain');
            const selection = {
                empty: () => false,
                datum: () => ({ id: 'pg-child', resolvedExecutionEngine: 'STANDARD', component: {} })
            };

            expect(drain.condition!(selection as never)).toBe(true);
            drain.action!(selection as never);

            expect(dispatchSpy).toHaveBeenCalledWith(promptDrain({ request: { processGroupId: 'pg-child' } }));
        });

        it('is hidden when the current group is STATELESS', async () => {
            const { service } = await setup({ resolvedExecutionEngine: 'STATELESS' });
            const drain = menuItem(service.getMenu('root')!.menuItems, 'Drain');

            expect(drain.condition!({ empty: () => true } as never)).toBe(false);
        });

        it('is hidden when the user cannot modify the current group', async () => {
            const { service } = await setup({ canModifyCurrentProcessGroup: false });
            const drain = menuItem(service.getMenu('root')!.menuItems, 'Drain');

            expect(drain.condition!({ empty: () => true } as never)).toBe(false);
        });

        it('is hidden when a drain is already running on the current group', async () => {
            const { service } = await setup({ currentDrainRequestId: 'drain-1' });
            const drain = menuItem(service.getMenu('root')!.menuItems, 'Drain');

            expect(drain.condition!({ empty: () => true } as never)).toBe(false);
        });

        it('is hidden when the selection is not a process group', async () => {
            const { service } = await setup({ isProcessGroup: false });
            const drain = menuItem(service.getMenu('root')!.menuItems, 'Drain');

            expect(drain.condition!({ empty: () => false } as never)).toBe(false);
        });
    });

    describe('View Drain', () => {
        it('is visible on an empty canvas when a drain is running and the user can read', async () => {
            const { service, dispatchSpy } = await setup({
                currentProcessGroupId: 'current-pg',
                currentDrainRequestId: 'drain-1'
            });
            const viewDrainItem = menuItem(service.getMenu('root')!.menuItems, 'View Drain');
            const selection = { empty: () => true };

            expect(viewDrainItem.condition!(selection as never)).toBe(true);
            viewDrainItem.action!(selection as never);

            expect(dispatchSpy).toHaveBeenCalledWith(
                viewDrain({ request: { processGroupId: 'current-pg', drainRequestId: 'drain-1' } })
            );
        });

        it('is visible for a selected process group that has a drain summary', async () => {
            const { service, dispatchSpy } = await setup({ isProcessGroup: true });
            const viewDrainItem = menuItem(service.getMenu('root')!.menuItems, 'View Drain');
            const selection = {
                empty: () => false,
                datum: () => ({
                    id: 'pg-child',
                    drainSummary: { id: 'drain-9' }
                })
            };

            expect(viewDrainItem.condition!(selection as never)).toBe(true);
            viewDrainItem.action!(selection as never);

            expect(dispatchSpy).toHaveBeenCalledWith(
                viewDrain({ request: { processGroupId: 'pg-child', drainRequestId: 'drain-9' } })
            );
        });

        it('is hidden when the user cannot read the current group', async () => {
            const { service } = await setup({
                canReadCurrentProcessGroup: false,
                currentDrainRequestId: 'drain-1'
            });
            const viewDrainItem = menuItem(service.getMenu('root')!.menuItems, 'View Drain');

            expect(viewDrainItem.condition!({ empty: () => true } as never)).toBe(false);
        });

        it('is hidden for an unreadable selected process group that is draining', async () => {
            const { service } = await setup({ isProcessGroup: true, canRead: false });
            const viewDrainItem = menuItem(service.getMenu('root')!.menuItems, 'View Drain');
            const selection = {
                empty: () => false,
                datum: () => ({
                    id: 'pg-child',
                    drainSummary: { id: 'drain-9' }
                })
            };

            expect(viewDrainItem.condition!(selection as never)).toBe(false);
        });
    });
});
