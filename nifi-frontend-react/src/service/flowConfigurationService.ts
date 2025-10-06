// FlowConfigurationService migrated from Angular with RxJS
import { ajax } from 'rxjs/ajax';
import { map } from 'rxjs/operators';
import { Observable } from 'rxjs';

import type { FlowConfiguration } from '../state/flowConfigurationSlice';

export const API = '../nifi-api';

export interface FlowConfigurationResponse {
  flowConfiguration: FlowConfiguration;
  [key: string]: unknown;
}

export function getFlowConfiguration$(): Observable<FlowConfigurationResponse> {
  return ajax
    .getJSON<FlowConfigurationResponse>(`${API}/flow/config`)
    .pipe(map((response) => response));
}
