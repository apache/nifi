// PropertyVerificationService migrated from Angular with RxJS
import { ajax } from 'rxjs/ajax';
import { map } from 'rxjs/operators';
import { Observable } from 'rxjs';

export const API = '../nifi-api';

export interface ConfigurationAnalysisResponse {
  analysis?: any;
}

export function getAnalysis$(request: any): Observable<ConfigurationAnalysisResponse> {
  const body = {
    configurationAnalysis: request
  };
  return ajax.post(`${API}/property-verification/analysis`, body, {
    'Content-Type': 'application/json'
  }).pipe(map(response => response.response));
}
