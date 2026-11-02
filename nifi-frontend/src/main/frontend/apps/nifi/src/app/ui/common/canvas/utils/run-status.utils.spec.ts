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

import { ComponentType } from '@nifi/shared';
import { describe, expect, it } from 'vitest';
import { SelectionTarget } from './selection.utils';
import {
    areAllDisableable,
    areAllEnableable,
    areAnyRunnable,
    areAnyStoppable,
    canRunOnce,
    canTerminate
} from './run-status.utils';

function processor(runStatus: string, activeThreadCount = 0, canWrite = true): SelectionTarget {
    return {
        entity: {
            id: 'processor-1',
            permissions: { canRead: true, canWrite },
            revision: { version: 1 },
            status: { aggregateSnapshot: { runStatus, activeThreadCount } },
            component: {}
        },
        ui: { componentType: ComponentType.Processor }
    } as SelectionTarget;
}

function processGroup(canRead: boolean): SelectionTarget {
    return {
        entity: {
            id: 'process-group-1',
            permissions: { canRead, canWrite: false },
            revision: { version: 1 }
        },
        ui: { componentType: ComponentType.ProcessGroup }
    } as SelectionTarget;
}

describe('run status utilities', () => {
    it('allows an operable stopped processor to start, disable, and run once', () => {
        const selection = [processor('Stopped')];

        expect(areAnyRunnable(selection)).toBe(true);
        expect(areAllDisableable(selection)).toBe(true);
        expect(canRunOnce(selection)).toBe(true);
    });

    it('allows an operable running processor to stop', () => {
        expect(areAnyStoppable([processor('Running')])).toBe(true);
    });

    it('allows an operable disabled processor to enable', () => {
        expect(areAllEnableable([processor('Disabled')])).toBe(true);
    });

    it('requires every selected component to be enableable', () => {
        expect(areAllEnableable([processor('Disabled'), processor('Stopped')])).toBe(false);
    });

    it('requires one non-running processor with active threads to terminate', () => {
        const selection = [processor('Stopped', 2)];

        expect(canTerminate(selection)).toBe(true);
        expect(canTerminate([...selection, selection[0]])).toBe(false);
        expect(canTerminate([processor('Running', 2)])).toBe(false);
    });

    it('rejects mutations without write or operate permission', () => {
        expect(areAnyRunnable([processor('Stopped', 0, false)])).toBe(false);
    });

    it('accepts a mixed selection when any target is eligible', () => {
        expect(areAnyRunnable([processor('Stopped'), processor('Running')])).toBe(true);
    });

    it('delegates process-group authorization to the flow endpoint', () => {
        expect(areAnyRunnable([processGroup(true)])).toBe(true);
        expect(areAnyStoppable([processGroup(false)])).toBe(true);
        expect(areAllEnableable([processGroup(false)])).toBe(true);
        expect(areAllDisableable([processGroup(false)])).toBe(true);
    });

    it('does not enable a disabled processor while it has active threads', () => {
        expect(areAllEnableable([processor('Disabled', 1)])).toBe(false);
    });

    it('does not disable a stopped processor while it has active threads', () => {
        expect(areAllDisableable([processor('Stopped', 1)])).toBe(false);
    });

    it('does not disable an invalid processor while it has active threads', () => {
        expect(areAllDisableable([processor('Invalid', 1)])).toBe(false);
    });
});
