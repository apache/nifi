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
import * as FlowActions from '../state/flow/flow.actions';
import type { ResolvedExecutionEngine } from '../state/flow';
import type { ContextMenuItemDefinition } from '../../../ui/common/context-menu/context-menu.component';

interface SetupOptions {
    currentProcessGroupId?: string;
    isProcessGroup?: boolean;
    resolvedExecutionEngine?: ResolvedExecutionEngine;
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
        getResolvedExecutionEngine: vi.fn().mockReturnValue(options.resolvedExecutionEngine ?? 'STANDARD')
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
    describe('Stop sources', () => {
        it('is immediately after Stop in the root menu', async () => {
            const { service } = await setup();
            const menuItems = service.getMenu('root')!.menuItems;
            const texts = menuItems.map((item) => item.text);
            const stopIndex = texts.indexOf('Stop');
            const stopSourcesIndex = texts.indexOf('Stop sources');

            expect(stopIndex).toBeGreaterThan(-1);
            expect(stopSourcesIndex).toBe(stopIndex + 1);
        });

        it('is visible on an empty canvas and dispatches stopSources for the current group', async () => {
            const { service, canvasUtils, dispatchSpy } = await setup({ currentProcessGroupId: 'current-pg' });
            const stopSources = menuItem(service.getMenu('root')!.menuItems, 'Stop sources');
            const selection = { empty: () => true };

            expect(stopSources.condition!(selection as never)).toBe(true);
            stopSources.action!(selection as never);

            expect(canvasUtils.getProcessGroupId).toHaveBeenCalled();
            expect(dispatchSpy).toHaveBeenCalledWith(FlowActions.stopSources({ request: { id: 'current-pg' } }));
        });

        it('is visible for a selected process group and dispatches stopSources for that group', async () => {
            const { service, dispatchSpy } = await setup({ isProcessGroup: true });
            const stopSources = menuItem(service.getMenu('root')!.menuItems, 'Stop sources');
            const selection = {
                empty: () => false,
                datum: () => ({ id: 'pg-child', resolvedExecutionEngine: 'STANDARD' })
            };

            expect(stopSources.condition!(selection as never)).toBe(true);
            stopSources.action!(selection as never);

            expect(dispatchSpy).toHaveBeenCalledWith(FlowActions.stopSources({ request: { id: 'pg-child' } }));
        });

        it('is hidden when the selection is not a process group', async () => {
            const { service } = await setup({ isProcessGroup: false });
            const stopSources = menuItem(service.getMenu('root')!.menuItems, 'Stop sources');
            const selection = { empty: () => false };

            expect(stopSources.condition!(selection as never)).toBe(false);
        });

        it('is hidden on an empty canvas when the current group resolves to STATELESS', async () => {
            const { service } = await setup({ resolvedExecutionEngine: 'STATELESS' });
            const stopSources = menuItem(service.getMenu('root')!.menuItems, 'Stop sources');
            const selection = { empty: () => true };

            expect(stopSources.condition!(selection as never)).toBe(false);
        });

        it('is hidden for a selected process group when the resolved engine is missing', async () => {
            const { service } = await setup({ isProcessGroup: true });
            const stopSources = menuItem(service.getMenu('root')!.menuItems, 'Stop sources');
            const selection = {
                empty: () => false,
                datum: () => ({ id: 'pg-child' })
            };

            expect(stopSources.condition!(selection as never)).toBe(false);
        });

        it('is visible on an empty canvas when the current group is configured INHERITED but resolves to STANDARD', async () => {
            const { service } = await setup({ resolvedExecutionEngine: 'STANDARD' });
            const stopSources = menuItem(service.getMenu('root')!.menuItems, 'Stop sources');
            const selection = { empty: () => true };

            expect(stopSources.condition!(selection as never)).toBe(true);
        });

        it('is visible for a selected process group with no component when the entity resolves to STANDARD', async () => {
            const { service } = await setup({ isProcessGroup: true });
            const stopSources = menuItem(service.getMenu('root')!.menuItems, 'Stop sources');
            const selection = {
                empty: () => false,
                datum: () => ({ id: 'pg-child', resolvedExecutionEngine: 'STANDARD' })
            };

            expect(stopSources.condition!(selection as never)).toBe(true);
        });

        it('is hidden for a selected process group with no component when the entity resolves to STATELESS', async () => {
            const { service } = await setup({ isProcessGroup: true });
            const stopSources = menuItem(service.getMenu('root')!.menuItems, 'Stop sources');
            const selection = {
                empty: () => false,
                datum: () => ({ id: 'pg-child', resolvedExecutionEngine: 'STATELESS' })
            };

            expect(stopSources.condition!(selection as never)).toBe(false);
        });
    });
});
