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

import { Injectable, inject } from '@angular/core';
import { Store } from '@ngrx/store';
import { ComponentType } from '@nifi/shared';
import { Client } from '../../../../service/client.service';
import { SelectionTarget } from '../../../../ui/common/canvas/utils/selection.utils';
import {
    getDisableable,
    getEnableable,
    getStartable,
    getStoppable
} from '../../../../ui/common/canvas/utils/run-status.utils';
import * as ConnectorCanvasActions from '../../state/connector-canvas/connector-canvas.actions';
import { selectProcessGroupId } from '../../state/connector-canvas/connector-canvas.selectors';

@Injectable()
export class ConnectorCanvasCommands {
    private readonly store = inject(Store);
    private readonly client = inject(Client);
    private readonly processGroupId = this.store.selectSignal(selectProcessGroupId);
    startComponents(candidates: SelectionTarget[]): void {
        if (candidates.length === 0) {
            const id = this.processGroupId();
            if (id) {
                this.store.dispatch(ConnectorCanvasActions.startCurrentProcessGroup());
            }
            return;
        }
        const components = getStartable(candidates).map((candidate) => ({
            id: candidate.entity.id,
            type: candidate.ui.componentType,
            revision: this.client.getRevision(candidate.entity),
            errorStrategy: 'banner' as const
        }));
        if (components.length > 0) {
            this.store.dispatch(ConnectorCanvasActions.startComponents({ request: { components } }));
        }
    }

    stopComponents(candidates: SelectionTarget[]): void {
        if (candidates.length === 0) {
            const id = this.processGroupId();
            if (id) {
                this.store.dispatch(ConnectorCanvasActions.stopCurrentProcessGroup());
            }
            return;
        }
        const components = getStoppable(candidates).map((candidate) => ({
            id: candidate.entity.id,
            type: candidate.ui.componentType,
            revision: this.client.getRevision(candidate.entity),
            errorStrategy: 'banner' as const
        }));
        if (components.length > 0) {
            this.store.dispatch(ConnectorCanvasActions.stopComponents({ request: { components } }));
        }
    }

    enableComponents(candidates: SelectionTarget[]): void {
        if (candidates.length === 0) {
            const id = this.processGroupId();
            if (id) {
                this.store.dispatch(
                    ConnectorCanvasActions.enableComponent({
                        request: {
                            id,
                            type: ComponentType.ProcessGroup,
                            errorStrategy: 'banner'
                        }
                    })
                );
            }
            return;
        }
        if (getEnableable(candidates).length !== candidates.length) {
            return;
        }
        const components = candidates.map((candidate) => ({
            id: candidate.entity.id,
            type: candidate.ui.componentType,
            revision: this.client.getRevision(candidate.entity),
            errorStrategy: 'banner' as const
        }));
        if (components.length > 0) {
            this.store.dispatch(ConnectorCanvasActions.enableComponents({ request: { components } }));
        }
    }

    disableComponents(candidates: SelectionTarget[]): void {
        if (candidates.length === 0) {
            const id = this.processGroupId();
            if (id) {
                this.store.dispatch(
                    ConnectorCanvasActions.disableComponent({
                        request: {
                            id,
                            type: ComponentType.ProcessGroup,
                            errorStrategy: 'banner'
                        }
                    })
                );
            }
            return;
        }
        if (getDisableable(candidates).length !== candidates.length) {
            return;
        }
        const components = candidates.map((candidate) => ({
            id: candidate.entity.id,
            type: candidate.ui.componentType,
            revision: this.client.getRevision(candidate.entity),
            errorStrategy: 'banner' as const
        }));
        if (components.length > 0) {
            this.store.dispatch(ConnectorCanvasActions.disableComponents({ request: { components } }));
        }
    }

    runOnce(candidates: SelectionTarget[]): void {
        if (candidates.length === 1 && candidates[0].ui.componentType === ComponentType.Processor) {
            this.store.dispatch(
                ConnectorCanvasActions.runOnceComponent({
                    request: {
                        id: candidates[0].entity.id,
                        revision: this.client.getRevision(candidates[0].entity)
                    }
                })
            );
        }
    }

    terminateThreads(candidates: SelectionTarget[]): void {
        if (candidates.length === 1 && candidates[0].ui.componentType === ComponentType.Processor) {
            this.store.dispatch(
                ConnectorCanvasActions.terminateComponentThreads({
                    request: { id: candidates[0].entity.id, uri: candidates[0].entity.uri }
                })
            );
        }
    }

    openAdvancedProcessor(id: string): void {
        this.store.dispatch(ConnectorCanvasActions.navigateToAdvancedProcessorUi({ id }));
    }
}
