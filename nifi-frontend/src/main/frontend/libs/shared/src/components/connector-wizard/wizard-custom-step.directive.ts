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

import { Directive, input, TemplateRef, inject } from '@angular/core';

/**
 * Structural directive for projecting custom step templates into ConnectorWizard.
 * Each directive instance registers a step name and provides a template reference
 * that the wizard can render when that step is active.
 *
 * Usage:
 * ```html
 * <connector-wizard>
 *     <ng-template wizardCustomStep="Replication table schema" let-stepName>
 *         <my-custom-step [stepName]="stepName"></my-custom-step>
 *     </ng-template>
 * </connector-wizard>
 * ```
 */
@Directive({
    selector: '[wizardCustomStep]',
    standalone: true
})
export class WizardCustomStepDirective {
    readonly stepName = input.required<string>({ alias: 'wizardCustomStep' });

    readonly templateRef = inject<TemplateRef<{ $implicit: string }>>(TemplateRef);
}
