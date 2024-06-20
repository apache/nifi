import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class FlowAnalysisService {
  private static readonly API: string = '../nifi-api';

  constructor(private httpClient: HttpClient) { }

  getResults(processGroupId: string): Observable<any> {
    return this.httpClient.get(`${FlowAnalysisService.API}/flow/flow-analysis/results/${processGroupId}`)
  }
}
