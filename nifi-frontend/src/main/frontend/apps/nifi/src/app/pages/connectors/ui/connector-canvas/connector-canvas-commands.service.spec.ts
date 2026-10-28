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
import { ComponentType } from '@nifi/shared';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { Client } from '../../../../service/client.service';
import { SelectionTarget } from '../../../../ui/common/canvas/utils/selection.utils';
import * as ConnectorCanvasActions from '../../state/connector-canvas/connector-canvas.actions';
import { selectProcessGroupId } from '../../state/connector-canvas/connector-canvas.selectors';
import { ConnectorCanvasCommands } from './connector-canvas-commands.service';

function processor(runStatus = 'Stopped', activeThreadCount = 0, id = 'processor-1'): SelectionTarget {
    return {
        entity: {
            id,
            permissions: { canRead: true, canWrite: true },
            revision: { version: 4 },
            status: { aggregateSnapshot: { runStatus, activeThreadCount } },
            component: {}
        },
        ui: { componentType: ComponentType.Processor }
    } as SelectionTarget;
}

async function setup() {
    await TestBed.configureTestingModule({
        providers: [
            ConnectorCanvasCommands,
            provideMockStore({
                selectors: [{ selector: selectProcessGroupId, value: 'pg-1' }]
            }),
            { provide: Client, useValue: { getRevision: vi.fn(() => ({ version: 4 })) } }
        ]
    }).compileComponents();
    const store = TestBed.inject(MockStore);
    const dispatch = vi.spyOn(store, 'dispatch');
    return { commands: TestBed.inject(ConnectorCanvasCommands), dispatch };
}

describe('ConnectorCanvasCommands', () => {
    beforeEach(() => {
        TestBed.resetTestingModule();
        vi.clearAllMocks();
    });

    it('builds a typed start request with the current revision', async () => {
        const { commands, dispatch } = await setup();

        commands.startComponents([processor()]);

        expect(dispatch).toHaveBeenCalledWith(
            ConnectorCanvasActions.startComponents({
                request: {
                    components: [
                        {
                            id: 'processor-1',
                            type: ComponentType.Processor,
                            revision: { version: 4 },
                            errorStrategy: 'banner'
                        }
                    ]
                }
            })
        );
    });

    it('targets the current process group for an empty selection', async () => {
        const { commands, dispatch } = await setup();

        commands.startComponents([]);

        expect(dispatch).toHaveBeenCalledWith(ConnectorCanvasActions.startCurrentProcessGroup());
    });

    it('dispatches only the eligible subset of a mixed start selection', async () => {
        const { commands, dispatch } = await setup();

        commands.startComponents([processor('Stopped', 0, 'stopped'), processor('Running', 0, 'running')]);

        expect(dispatch).toHaveBeenCalledWith(
            ConnectorCanvasActions.startComponents({
                request: {
                    components: [
                        {
                            id: 'stopped',
                            type: ComponentType.Processor,
                            revision: { version: 4 },
                            errorStrategy: 'banner'
                        }
                    ]
                }
            })
        );
    });

    it('dispatches stop, enable, disable, run-once, and terminate requests', async () => {
        const { commands, dispatch } = await setup();
        const stopped = processor();
        const running = processor('Running');
        const disabled = processor('Disabled');
        const threaded = processor('Stopped', 1);

        commands.stopComponents([running]);
        commands.enableComponents([disabled]);
        commands.disableComponents([stopped]);
        commands.runOnce([stopped]);
        commands.terminateThreads([threaded]);

        expect(dispatch).toHaveBeenCalledWith(expect.objectContaining({ type: '[Connector Canvas] Stop Components' }));
        expect(dispatch).toHaveBeenCalledWith(
            expect.objectContaining({ type: '[Connector Canvas] Enable Components' })
        );
        expect(dispatch).toHaveBeenCalledWith(
            expect.objectContaining({ type: '[Connector Canvas] Disable Components' })
        );
        expect(dispatch).toHaveBeenCalledWith(
            expect.objectContaining({ type: '[Connector Canvas] Run Once Component' })
        );
        expect(dispatch).toHaveBeenCalledWith(
            expect.objectContaining({ type: '[Connector Canvas] Terminate Component Threads' })
        );
    });

    it('does not dispatch for a mixed enableable and non-enableable selection', async () => {
        const { commands, dispatch } = await setup();

        commands.enableComponents([processor('Disabled', 0, 'disabled'), processor('Stopped', 0, 'stopped')]);

        expect(dispatch).not.toHaveBeenCalled();
    });
});
