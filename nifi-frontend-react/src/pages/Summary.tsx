import React, { useState } from 'react';
import { BarChart3, Activity, Clock, Database, RefreshCw, TrendingUp, Zap } from 'lucide-react';

interface ProcessorSummary {
  id: string;
  name: string;
  type: string;
  state: 'RUNNING' | 'STOPPED' | 'DISABLED';
  inputCount: number;
  inputBytes: string;
  outputCount: number;
  outputBytes: string;
  readBytes: string;
  writtenBytes: string;
  tasks: number;
}

interface ConnectionSummary {
  id: string;
  name: string;
  sourceComponent: string;
  destinationComponent: string;
  queuedCount: number;
  queuedBytes: string;
  percentUse: number;
}

const Summary: React.FC = () => {
  const [activeTab, setActiveTab] = useState('processors');
  const [autoRefresh, setAutoRefresh] = useState(true);

  const mockProcessors: ProcessorSummary[] = [
    {
      id: '1',
      name: 'GenerateFlowFile',
      type: 'GenerateFlowFile',
      state: 'RUNNING',
      inputCount: 0,
      inputBytes: '0 B',
      outputCount: 1250,
      outputBytes: '2.5 MB',
      readBytes: '0 B',
      writtenBytes: '2.5 MB',
      tasks: 1
    },
    {
      id: '2',
      name: 'RouteOnAttribute',
      type: 'RouteOnAttribute',
      state: 'RUNNING',
      inputCount: 1250,
      inputBytes: '2.5 MB',
      outputCount: 1250,
      outputBytes: '2.5 MB',
      readBytes: '2.5 MB',
      writtenBytes: '2.5 MB',
      tasks: 1
    },
    {
      id: '3',
      name: 'PutFile',
      type: 'PutFile',
      state: 'RUNNING',
      inputCount: 1250,
      inputBytes: '2.5 MB',
      outputCount: 0,
      outputBytes: '0 B',
      readBytes: '2.5 MB',
      writtenBytes: '2.5 MB',
      tasks: 1
    },
    {
      id: '4',
      name: 'LogAttribute',
      type: 'LogAttribute',
      state: 'STOPPED',
      inputCount: 0,
      inputBytes: '0 B',
      outputCount: 0,
      outputBytes: '0 B',
      readBytes: '0 B',
      writtenBytes: '0 B',
      tasks: 0
    }
  ];

  const mockConnections: ConnectionSummary[] = [
    {
      id: '1',
      name: 'success',
      sourceComponent: 'GenerateFlowFile',
      destinationComponent: 'RouteOnAttribute',
      queuedCount: 0,
      queuedBytes: '0 B',
      percentUse: 0
    },
    {
      id: '2',
      name: 'matched',
      sourceComponent: 'RouteOnAttribute',
      destinationComponent: 'PutFile',
      queuedCount: 0,
      queuedBytes: '0 B',
      percentUse: 0
    },
    {
      id: '3',
      name: 'unmatched',
      sourceComponent: 'RouteOnAttribute',
      destinationComponent: 'LogAttribute',
      queuedCount: 125,
      queuedBytes: '250 KB',
      percentUse: 12.5
    }
  ];

  const getStateColor = (state: string) => {
    const colors: Record<string, string> = {
      'RUNNING': 'bg-green-100 text-green-800',
      'STOPPED': 'bg-gray-100 text-gray-800',
      'DISABLED': 'bg-red-100 text-red-800'
    };
    return colors[state] || 'bg-gray-100 text-gray-800';
  };

  const renderProcessorSummary = () => (
    <div className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="card p-4">
          <div className="flex items-center">
            <div className="flex-1">
              <p className="text-sm text-gray-600">Total Processors</p>
              <p className="text-2xl font-bold text-gray-900">4</p>
            </div>
            <Activity className="h-8 w-8 text-blue-500" />
          </div>
        </div>
        <div className="card p-4">
          <div className="flex items-center">
            <div className="flex-1">
              <p className="text-sm text-gray-600">Running</p>
              <p className="text-2xl font-bold text-green-600">3</p>
            </div>
            <Zap className="h-8 w-8 text-green-500" />
          </div>
        </div>
        <div className="card p-4">
          <div className="flex items-center">
            <div className="flex-1">
              <p className="text-sm text-gray-600">Stopped</p>
              <p className="text-2xl font-bold text-gray-600">1</p>
            </div>
            <Clock className="h-8 w-8 text-gray-500" />
          </div>
        </div>
        <div className="card p-4">
          <div className="flex items-center">
            <div className="flex-1">
              <p className="text-sm text-gray-600">Total I/O</p>
              <p className="text-2xl font-bold text-purple-600">7.5 MB</p>
            </div>
            <TrendingUp className="h-8 w-8 text-purple-500" />
          </div>
        </div>
      </div>

      <div className="card p-0 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Name</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Type</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">State</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Input</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Output</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Read</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Written</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Tasks</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {mockProcessors.map((processor) => (
                <tr key={processor.id} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="font-medium text-gray-900">{processor.name}</div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {processor.type}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${getStateColor(processor.state)}`}>
                      {processor.state}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    <div>{processor.inputCount.toLocaleString()}</div>
                    <div className="text-xs text-gray-400">{processor.inputBytes}</div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    <div>{processor.outputCount.toLocaleString()}</div>
                    <div className="text-xs text-gray-400">{processor.outputBytes}</div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {processor.readBytes}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {processor.writtenBytes}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {processor.tasks}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );

  const renderConnectionSummary = () => (
    <div className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="card p-4">
          <div className="flex items-center">
            <div className="flex-1">
              <p className="text-sm text-gray-600">Total Connections</p>
              <p className="text-2xl font-bold text-gray-900">3</p>
            </div>
            <Database className="h-8 w-8 text-blue-500" />
          </div>
        </div>
        <div className="card p-4">
          <div className="flex items-center">
            <div className="flex-1">
              <p className="text-sm text-gray-600">Queued Files</p>
              <p className="text-2xl font-bold text-orange-600">125</p>
            </div>
            <Activity className="h-8 w-8 text-orange-500" />
          </div>
        </div>
        <div className="card p-4">
          <div className="flex items-center">
            <div className="flex-1">
              <p className="text-sm text-gray-600">Queued Data</p>
              <p className="text-2xl font-bold text-purple-600">250 KB</p>
            </div>
            <TrendingUp className="h-8 w-8 text-purple-500" />
          </div>
        </div>
      </div>

      <div className="card p-0 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Name</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Source</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Destination</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Queued</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Queue Usage</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {mockConnections.map((connection) => (
                <tr key={connection.id} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="font-medium text-gray-900">{connection.name}</div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {connection.sourceComponent}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {connection.destinationComponent}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    <div>{connection.queuedCount.toLocaleString()}</div>
                    <div className="text-xs text-gray-400">{connection.queuedBytes}</div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center">
                      <div className="w-full bg-gray-200 rounded-full h-2">
                        <div 
                          className={`h-2 rounded-full ${connection.percentUse > 80 ? 'bg-red-600' : connection.percentUse > 50 ? 'bg-yellow-600' : 'bg-green-600'}`}
                          style={{ width: `${connection.percentUse}%` }}
                        ></div>
                      </div>
                      <span className="ml-2 text-sm text-gray-600">{connection.percentUse}%</span>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="bg-white border-b px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <BarChart3 className="h-6 w-6 text-gray-500" />
            <h1 className="text-2xl font-bold text-gray-900">Summary</h1>
          </div>
          <div className="flex items-center space-x-4">
            <div className="flex items-center space-x-2">
              <input
                type="checkbox"
                id="auto-refresh"
                checked={autoRefresh}
                onChange={(e) => setAutoRefresh(e.target.checked)}
                className="rounded border-gray-300 text-primary-600 focus:ring-primary-500"
              />
              <label htmlFor="auto-refresh" className="text-sm text-gray-600">Auto Refresh</label>
            </div>
            <button className="btn-outline px-4 py-2 rounded-md flex items-center space-x-2">
              <RefreshCw className="h-4 w-4" />
              <span>Refresh</span>
            </button>
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="bg-white border-b">
        <div className="px-6">
          <nav className="flex space-x-8">
            <button
              onClick={() => setActiveTab('processors')}
              className={`flex items-center space-x-2 py-4 px-1 border-b-2 font-medium text-sm ${
                activeTab === 'processors'
                  ? 'border-primary-500 text-primary-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              <Activity className="h-4 w-4" />
              <span>Processors</span>
            </button>
            <button
              onClick={() => setActiveTab('connections')}
              className={`flex items-center space-x-2 py-4 px-1 border-b-2 font-medium text-sm ${
                activeTab === 'connections'
                  ? 'border-primary-500 text-primary-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              <Database className="h-4 w-4" />
              <span>Connections</span>
            </button>
          </nav>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 bg-gray-50 p-6 overflow-auto">
        {activeTab === 'processors' ? renderProcessorSummary() : renderConnectionSummary()}
      </div>
    </div>
  );
};

export default Summary;