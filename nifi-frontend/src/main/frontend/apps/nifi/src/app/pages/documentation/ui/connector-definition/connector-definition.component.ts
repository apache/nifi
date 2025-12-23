/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, OnDestroy, ViewChild, inject } from '@angular/core';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { ComponentType, isDefinedAndNotNull, NiFiCommon } from '@nifi/shared';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { selectDefinitionCoordinatesFromRouteForComponentType } from '../../state/documentation/documentation.selectors';
import { distinctUntilChanged } from 'rxjs';
import {
    ConnectorDefinitionState,
    ConfigurationStep,
    ConnectorPropertyGroup,
    ConnectorPropertyDescriptor,
    ConnectorDefinition,
    StepDocumentationState
} from '../../state/connector-definition';
import {
    loadConnectorDefinition,
    loadStepDocumentation,
    resetConnectorDefinitionState
} from '../../state/connector-definition/connector-definition.actions';
import { selectConnectorDefinitionState } from '../../state/connector-definition/connector-definition.selectors';
import { MatAccordion, MatExpansionModule } from '@angular/material/expansion';
import { MatButtonModule } from '@angular/material/button';
import { SeeAlsoComponent } from '../common/see-also/see-also.component';
import { MarkdownComponent } from 'ngx-markdown';
import { ConnectorPropertyDefinitionComponent } from '../common/connector-property-definition/connector-property-definition.component';

@Component({
    selector: 'connector-definition',
    imports: [
        NgxSkeletonLoaderModule,
        MatExpansionModule,
        MatButtonModule,
        SeeAlsoComponent,
        MarkdownComponent,
        ConnectorPropertyDefinitionComponent
    ],
    templateUrl: './connector-definition.component.html',
    styleUrl: './connector-definition.component.scss'
})
export class ConnectorDefinitionComponent implements OnDestroy {
    private store = inject<Store<NiFiState>>(Store);
    private nifiCommon = inject(NiFiCommon);

    @ViewChild('stepsAccordion') stepsAccordion!: MatAccordion;

    connectorDefinitionState: ConnectorDefinitionState | null = null;

    constructor() {
        this.store
            .select(selectDefinitionCoordinatesFromRouteForComponentType(ComponentType.Connector))
            .pipe(
                isDefinedAndNotNull(),
                distinctUntilChanged(
                    (a, b) =>
                        a.group === b.group && a.artifact === b.artifact && a.version === b.version && a.type === b.type
                ),
                takeUntilDestroyed()
            )
            .subscribe((coordinates) => {
                this.store.dispatch(
                    loadConnectorDefinition({
                        coordinates
                    })
                );
            });

        this.store
            .select(selectConnectorDefinitionState)
            .pipe(takeUntilDestroyed())
            .subscribe((connectorDefinitionState) => {
                const previousState = this.connectorDefinitionState;
                this.connectorDefinitionState = connectorDefinitionState;

                if (connectorDefinitionState.status === 'loading') {
                    window.scrollTo({ top: 0, left: 0 });
                }

                if (
                    previousState?.status !== 'success' &&
                    connectorDefinitionState.status === 'success' &&
                    connectorDefinitionState.connectorDefinition
                ) {
                    const firstStep = connectorDefinitionState.connectorDefinition.configurationSteps?.[0];
                    if (firstStep?.documented) {
                        this.loadStepDocumentation(connectorDefinitionState.connectorDefinition, firstStep.name);
                    }
                }
            });
    }

    isInitialLoading(state: ConnectorDefinitionState): boolean {
        return state.connectorDefinition === null && state.error === null;
    }

    formatExtensionName(type: string): string {
        return this.nifiCommon.getComponentTypeLabel(type);
    }

    hasConfigurationSteps(steps: ConfigurationStep[] | undefined): boolean {
        return steps !== undefined && steps.length > 0;
    }

    hasPropertyGroups(groups: ConnectorPropertyGroup[] | undefined): boolean {
        return groups !== undefined && groups.length > 0;
    }

    hasProperties(properties: ConnectorPropertyDescriptor[] | undefined): boolean {
        return properties !== undefined && properties.length > 0;
    }

    formatPropertyTitle(descriptor: ConnectorPropertyDescriptor): string {
        if (descriptor.required) {
            return `${descriptor.name}*`;
        }
        return descriptor.name;
    }

    lookupProperty(
        properties: ConnectorPropertyDescriptor[]
    ): (name: string) => ConnectorPropertyDescriptor | undefined {
        return (name: string) => properties.find((prop) => prop.name === name);
    }

    expandAllSteps(): void {
        this.stepsAccordion.openAll();
    }

    collapseAllSteps(): void {
        this.stepsAccordion.closeAll();
    }

    expandAllProperties(accordion: MatAccordion): void {
        accordion.openAll();
    }

    collapseAllProperties(accordion: MatAccordion): void {
        accordion.closeAll();
    }

    onStepExpanded(connectorDefinition: ConnectorDefinition, step: ConfigurationStep): void {
        if (step.documented) {
            this.loadStepDocumentation(connectorDefinition, step.name);
        }
    }

    loadStepDocumentation(connectorDefinition: ConnectorDefinition, stepName: string): void {
        const stepState = this.getStepDocumentationState(stepName);
        if (stepState && (stepState.status === 'loading' || stepState.status === 'success')) {
            return;
        }

        this.store.dispatch(
            loadStepDocumentation({
                coordinates: {
                    group: connectorDefinition.group,
                    artifact: connectorDefinition.artifact,
                    version: connectorDefinition.version,
                    type: connectorDefinition.type
                },
                stepName
            })
        );
    }

    getStepDocumentationState(stepName: string): StepDocumentationState | undefined {
        return this.connectorDefinitionState?.stepDocumentation[stepName];
    }

    isStepDocumentationLoading(stepName: string): boolean {
        const state = this.getStepDocumentationState(stepName);
        return state?.status === 'loading';
    }

    getStepDocumentation(stepName: string): string | undefined {
        return this.getStepDocumentationState(stepName)?.documentation ?? undefined;
    }

    getStepDocumentationError(stepName: string): string | undefined {
        return this.getStepDocumentationState(stepName)?.error ?? undefined;
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetConnectorDefinitionState());
    }
}
