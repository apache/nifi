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

import { Component, Input } from '@angular/core';
import { PropertyValueTipInput } from '../../../../state/shared';
import { Parameter } from '@nifi/shared';

@Component({
    selector: 'property-value-tip',
    standalone: true,
    templateUrl: './property-value-tip.component.html',
    styleUrl: './property-value-tip.component.scss'
})
export class PropertyValueTip {
    private _data: PropertyValueTipInput | undefined;
    private parameterRegex = new RegExp('^$');

    @Input() set data(data: PropertyValueTipInput | undefined) {
        this._data = data;
        this.extractParameterReferences();
    }
    get data(): PropertyValueTipInput | undefined {
        return this._data;
    }

    parameterReferences: Parameter[] = [];

    private extractParameterReferences() {
        if (this._data?.property.descriptor.sensitive) {
            return;
        }

        const propertyValue = this._data?.property.value || null;

        // get all the non-sensitive parameters
        const parameters = this.data?.parameters
            .filter((parameter) => !parameter.parameter.sensitive)
            .map((parameter) => parameter.parameter);

        if (propertyValue && parameters && parameters.length > 0) {
            this.parameterReferences = [];

            // build up the regex that will match any parameter in a string, even if it is quoted
            const allParamsRegex = parameters.reduce((regex, param, idx) => {
                if (idx > 0) {
                    regex += '|';
                }
                const quoteCaptureGroupIndex = idx * 2 + 1;
                regex += `#{(['"]?)(${param.name})\\${quoteCaptureGroupIndex}}`;
                return regex;
            }, '');
            this.parameterRegex = new RegExp(allParamsRegex, 'gm');

            const addedParameterNames = new Set<string>();

            let matched;
            while ((matched = this.parameterRegex.exec(propertyValue)) !== null) {
                // pull out the parameter name matched from the capturing groups, ignore any quote group
                const paramName = matched.splice(1).find((match) => !!match && match !== "'" && match !== '"');

                // get the Parameter object that was matched
                const param = parameters.find((param) => param.name === paramName);

                // if matched and not already added, add it to the list of parameter references
                if (param && !addedParameterNames.has(param.name)) {
                    this.parameterReferences.push(param);
                    addedParameterNames.add(param.name);
                }
            }
        }
    }
}
