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

import { Component, forwardRef } from '@angular/core';
import { ControlValueAccessor, FormsModule, NG_VALUE_ACCESSOR, ReactiveFormsModule } from '@angular/forms';
import { MatSliderModule } from '@angular/material/slider';

import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatRadioModule } from '@angular/material/radio';
import { NiFiCommon, NifiTooltipDirective, TextTip } from '@nifi/shared';
import { MatIconModule } from '@angular/material/icon';
import { Relationship } from '../../../../../../state/flow';

export interface RelationshipConfiguration {
    relationships: Relationship[];
    backoffMechanism: 'PENALIZE_FLOWFILE' | 'YIELD_PROCESSOR';
    maxBackoffPeriod: string;
    retryCount: number;
}

@Component({
    selector: 'relationship-settings',
    templateUrl: './relationship-settings.component.html',
    imports: [
        MatSliderModule,
        MatCheckboxModule,
        FormsModule,
        MatFormFieldModule,
        MatInputModule,
        ReactiveFormsModule,
        MatRadioModule,
        NifiTooltipDirective,
        MatIconModule
    ],
    styleUrls: ['./relationship-settings.component.scss'],
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            useExisting: forwardRef(() => RelationshipSettings),
            multi: true
        }
    ]
})
export class RelationshipSettings implements ControlValueAccessor {
    protected readonly TextTip = TextTip;

    relationships!: Relationship[];
    backoffMechanism!: 'PENALIZE_FLOWFILE' | 'YIELD_PROCESSOR';
    maxBackoffPeriod!: string;
    retryCount!: number;

    isDisabled = false;
    isTouched = false;
    onTouched!: () => void;
    onChange!: (relationshipConfiguration: RelationshipConfiguration) => void;

    constructor(private nifiCommon: NiFiCommon) {}

    registerOnChange(onChange: (relationshipConfiguration: RelationshipConfiguration) => void): void {
        this.onChange = onChange;
    }

    registerOnTouched(onTouch: () => void): void {
        this.onTouched = onTouch;
    }

    setDisabledState(isDisabled: boolean): void {
        this.isDisabled = isDisabled;
    }

    writeValue(relationshipConfiguration: RelationshipConfiguration): void {
        this.relationships = relationshipConfiguration.relationships.map((relationship) => {
            return { ...relationship };
        });
        this.backoffMechanism = relationshipConfiguration.backoffMechanism;
        this.maxBackoffPeriod = relationshipConfiguration.maxBackoffPeriod;
        this.retryCount = relationshipConfiguration.retryCount;
    }

    hasDescription(relationship: Relationship): boolean {
        return !this.nifiCommon.isBlank(relationship.description);
    }

    isRelationshipRetried(): boolean {
        if (this.relationships) {
            return this.relationships.some((relationship) => relationship.retry);
        }

        return false;
    }

    handleChanged(): void {
        // mark the component as touched if not already
        if (!this.isTouched) {
            this.isTouched = true;
            this.onTouched();
        }

        // emit the changes
        this.onChange(this.serializeRelationshipConfiguration());
    }

    private serializeRelationshipConfiguration(): RelationshipConfiguration {
        return {
            relationships: [...this.relationships],
            backoffMechanism: this.backoffMechanism,
            maxBackoffPeriod: this.maxBackoffPeriod,
            retryCount: this.retryCount
        };
    }
}
