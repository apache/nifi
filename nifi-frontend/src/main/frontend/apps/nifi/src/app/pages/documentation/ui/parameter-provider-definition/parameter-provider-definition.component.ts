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

import { Component, OnDestroy } from '@angular/core';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { ComponentType, isDefinedAndNotNull } from '@nifi/shared';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ConfigurableExtensionDefinitionComponent } from '../common/configurable-extension-definition/configurable-extension-definition.component';
import { selectDefinitionCoordinatesFromRouteForComponentType } from '../../state/documentation/documentation.selectors';
import { distinctUntilChanged } from 'rxjs';
import { ParameterProviderDefinitionState } from '../../state/parameter-provider-definition';
import {
    loadParameterProviderDefinition,
    resetParameterProviderDefinitionState
} from '../../state/parameter-provider-definition/parameter-provider-definition.actions';
import { selectParameterProviderDefinitionState } from '../../state/parameter-provider-definition/parameter-provider-definition.selectors';

@Component({
    selector: 'parameter-provider-definition',
    imports: [NgxSkeletonLoaderModule, ConfigurableExtensionDefinitionComponent],
    templateUrl: './parameter-provider-definition.component.html',
    styleUrl: './parameter-provider-definition.component.scss'
})
export class ParameterProviderDefinition implements OnDestroy {
    parameterProviderDefinitionState: ParameterProviderDefinitionState | null = null;

    constructor(private store: Store<NiFiState>) {
        this.store
            .select(selectDefinitionCoordinatesFromRouteForComponentType(ComponentType.ParameterProvider))
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
                    loadParameterProviderDefinition({
                        coordinates
                    })
                );
            });

        this.store
            .select(selectParameterProviderDefinitionState)
            .pipe(takeUntilDestroyed())
            .subscribe((parameterProviderDefinitionState) => {
                this.parameterProviderDefinitionState = parameterProviderDefinitionState;

                if (parameterProviderDefinitionState.status === 'loading') {
                    window.scrollTo({ top: 0, left: 0 });
                }
            });
    }

    isInitialLoading(state: ParameterProviderDefinitionState): boolean {
        return state.parameterProviderDefinition === null && state.error === null;
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetParameterProviderDefinitionState());
    }
}
