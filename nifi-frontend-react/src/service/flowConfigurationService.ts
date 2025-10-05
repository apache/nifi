// FlowConfigurationService migrated from Angular with RxJS
import { ajax } from 'rxjs/ajax';
import { map } from 'rxjs/operators';
import { Observable } from 'rxjs';

export const API = '../nifi-api';

export interface FlowConfigurationResponse {
  config?: any;
}

export function getFlowConfiguration$(): Observable<FlowConfigurationResponse> {
  return ajax.getJSON<FlowConfigurationResponse>(`${API}/flow/config`).pipe(
    map(response => response)
  );
}
