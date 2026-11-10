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
import { provideMockStore } from '@ngrx/store/testing';
import { ComponentType, Permissions } from '@nifi/shared';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { ContextMenuContext } from '../../../../ui/common/canvas/canvas.types';
import { ContextMenuItemDefinition } from '../../../../ui/common/context-menu/context-menu.component';
import { SelectionTarget } from '../../../../ui/common/canvas/utils/selection.utils';
import { selectConnectorCanvasPermissions } from '../../state/connector-canvas/connector-canvas.selectors';
import {
    selectConnectorCanvasEntity,
    selectConnectorCanvasEntitySaving
} from '../../state/connector-canvas-entity/connector-canvas-entity.selectors';
import { ConnectorCanvasCommands } from './connector-canvas-commands.service';
import { ConnectorCanvasContextMenu } from './connector-canvas-context-menu.service';

interface SetupOptions {
    state?: string;
    canvasPermissions?: Permissions | null;
}

function target(id: string, componentType: ComponentType, canWrite = true): SelectionTarget {
    return {
        entity: {
            id,
            permissions: { canRead: true, canWrite },
            revision: { version: 1 },
            component: {}
        },
        ui: { componentType }
    } as SelectionTarget;
}

function componentContext(clicked: SelectionTarget, selectedComponents: SelectionTarget[]): ContextMenuContext {
    return {
        targetType: 'component',
        clickedComponent: clicked,
        selectedComponents,
        processGroupId: 'pg-1'
    } as unknown as ContextMenuContext;
}

async function setup(options: SetupOptions = {}) {
    const commands = { delete: vi.fn() };
    await TestBed.configureTestingModule({
        providers: [
            ConnectorCanvasContextMenu,
            { provide: ConnectorCanvasCommands, useValue: commands },
            provideMockStore({
                selectors: [
                    {
                        selector: selectConnectorCanvasEntity,
                        value: {
                            permissions: { canRead: true },
                            component: { state: options.state ?? 'TROUBLESHOOTING' }
                        }
                    },
                    { selector: selectConnectorCanvasEntitySaving, value: false },
                    {
                        selector: selectConnectorCanvasPermissions,
                        value:
                            options.canvasPermissions === undefined
                                ? { canRead: true, canWrite: true }
                                : options.canvasPermissions
                    },
                    { selector: selectCurrentUser, value: { provenancePermissions: { canRead: true } } }
                ]
            })
        ]
    }).compileComponents();

    const contextMenu = TestBed.inject(ConnectorCanvasContextMenu);
    const host = {
        connectorId: () => 'connector-1',
        processGroupId: () => 'pg-1',
        canNavigateToParent: () => false,
        centerOnSelection: vi.fn()
    };
    contextMenu.setHost(host);

    const visibleItems = (context: ContextMenuContext): ContextMenuItemDefinition[] => {
        contextMenu.setContext(context);
        return (contextMenu.getMenu('root')?.menuItems ?? []).filter(
            (item) => !item.isSeparator && contextMenu.filterMenuItem(item)
        );
    };
    const deleteItem = (context: ContextMenuContext) => visibleItems(context).find((item) => item.text === 'Delete');

    return { contextMenu, commands, host, deleteItem };
}

describe('ConnectorCanvasContextMenu', () => {
    beforeEach(() => {
        TestBed.resetTestingModule();
        vi.clearAllMocks();
    });

    describe('Delete', () => {
        it('deletes the selection when editable and every selected component is writable', async () => {
            const { commands, deleteItem } = await setup();
            const selection = [target('proc-1', ComponentType.Processor), target('funnel-1', ComponentType.Funnel)];

            const item = deleteItem(componentContext(selection[0], selection));
            item?.action?.(null, new MouseEvent('click'));

            expect(item).toBeDefined();
            expect(commands.delete).toHaveBeenCalledWith(selection);
        });

        it('offers delete for an existing remote process group', async () => {
            const { deleteItem } = await setup();
            const rpg = target('rpg-1', ComponentType.RemoteProcessGroup);

            expect(deleteItem(componentContext(rpg, [rpg]))).toBeDefined();
        });

        it('deletes the clicked component when it is outside the selection', async () => {
            const { commands, deleteItem } = await setup();
            const clicked = target('proc-2', ComponentType.Processor);

            deleteItem(componentContext(clicked, [target('proc-1', ComponentType.Processor)]))?.action?.(
                null,
                new MouseEvent('click')
            );

            expect(commands.delete).toHaveBeenCalledWith([clicked]);
        });

        it('hides delete when any selected component is not writable', async () => {
            const { deleteItem } = await setup();
            const selection = [
                target('proc-1', ComponentType.Processor),
                target('proc-2', ComponentType.Processor, false)
            ];

            expect(deleteItem(componentContext(selection[0], selection))).toBeUndefined();
        });

        it('hides delete when a selected processor is running', async () => {
            const { deleteItem } = await setup();
            const running = {
                ...target('proc-1', ComponentType.Processor),
                entity: {
                    ...target('proc-1', ComponentType.Processor).entity,
                    status: { aggregateSnapshot: { runStatus: 'Running', activeThreadCount: 0 } }
                }
            } as SelectionTarget;

            expect(deleteItem(componentContext(running, [running]))).toBeUndefined();
        });

        it('hides delete outside troubleshooting', async () => {
            const { deleteItem } = await setup({ state: 'RUNNING' });
            const processor = target('proc-1', ComponentType.Processor);

            expect(deleteItem(componentContext(processor, [processor]))).toBeUndefined();
        });

        it('hides delete when the process group is not writable', async () => {
            const { deleteItem } = await setup({ canvasPermissions: { canRead: true, canWrite: false } });
            const processor = target('proc-1', ComponentType.Processor);

            expect(deleteItem(componentContext(processor, [processor]))).toBeUndefined();
        });
    });

    describe('host', () => {
        it('centers on the selection through the host', async () => {
            const { contextMenu, host } = await setup();

            contextMenu.centerInViewAction();

            expect(host.centerOnSelection).toHaveBeenCalled();
        });
    });
});
