import React, { useState } from 'react';
import { Settings as SettingsIcon, Server, Cpu, Database, FileText, RefreshCw, Plus } from 'lucide-react';

interface TabProps {
  id: string;
  label: string;
  icon: React.ReactNode;
  count?: number;
}

const Settings: React.FC = () => {
  const [activeTab, setActiveTab] = useState('controller-services');

  const tabs: TabProps[] = [
    { id: 'controller-services', label: 'Controller Services', icon: <Database className="h-4 w-4" />, count: 12 },
    { id: 'reporting-tasks', label: 'Reporting Tasks', icon: <FileText className="h-4 w-4" />, count: 3 },
    { id: 'registry-clients', label: 'Registry Clients', icon: <Server className="h-4 w-4" />, count: 1 },
    { id: 'parameters', label: 'Parameter Providers', icon: <SettingsIcon className="h-4 w-4" />, count: 0 },
  ];

  const mockControllerServices = [
    {
      id: '1',
      name: 'DatabaseConnectionPool',
      type: 'DBCPConnectionPool',
      state: 'ENABLED',
      scope: 'Controller',
      referencingComponents: 5
    },
    {
      id: '2', 
      name: 'DistributedMapCacheClient',
      type: 'DistributedMapCacheClientService',
      state: 'ENABLED',
      scope: 'Controller',
      referencingComponents: 2
    },
    {
      id: '3',
      name: 'StandardSSLContextService',
      type: 'StandardSSLContextService', 
      state: 'DISABLED',
      scope: 'Controller',
      referencingComponents: 0
    }
  ];

  const mockReportingTasks = [
    {
      id: '1',
      name: 'SiteToSiteProvenanceReportingTask',
      type: 'SiteToSiteProvenanceReportingTask',
      state: 'RUNNING',
      schedulingPeriod: '30 sec'
    },
    {
      id: '2',
      name: 'MonitorActivity',
      type: 'MonitorActivity',
      state: 'STOPPED',
      schedulingPeriod: '5 min'
    }
  ];

  const renderControllerServices = () => (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold">Controller Services</h3>
        <button className="btn-primary px-4 py-2 rounded-md flex items-center space-x-2">
          <Plus className="h-4 w-4" />
          <span>Add Service</span>
        </button>
      </div>
      
      <div className="card p-0 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Name</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Type</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">State</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Scope</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">References</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {mockControllerServices.map((service) => (
                <tr key={service.id} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="font-medium text-gray-900">{service.name}</div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {service.type}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${
                      service.state === 'ENABLED' 
                        ? 'bg-green-100 text-green-800' 
                        : 'bg-gray-100 text-gray-800'
                    }`}>
                      {service.state}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {service.scope}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {service.referencingComponents}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    <button className="btn-outline px-3 py-1 rounded text-xs mr-2">
                      Configure
                    </button>
                    <button className="btn-outline px-3 py-1 rounded text-xs">
                      {service.state === 'ENABLED' ? 'Disable' : 'Enable'}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );

  const renderReportingTasks = () => (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold">Reporting Tasks</h3>
        <button className="btn-primary px-4 py-2 rounded-md flex items-center space-x-2">
          <Plus className="h-4 w-4" />
          <span>Add Task</span>
        </button>
      </div>
      
      <div className="card p-0 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Name</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Type</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">State</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Schedule</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {mockReportingTasks.map((task) => (
                <tr key={task.id} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="font-medium text-gray-900">{task.name}</div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {task.type}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${
                      task.state === 'RUNNING' 
                        ? 'bg-green-100 text-green-800' 
                        : 'bg-gray-100 text-gray-800'
                    }`}>
                      {task.state}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {task.schedulingPeriod}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    <button className="btn-outline px-3 py-1 rounded text-xs mr-2">
                      Configure
                    </button>
                    <button className="btn-outline px-3 py-1 rounded text-xs">
                      {task.state === 'RUNNING' ? 'Stop' : 'Start'}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );

  const renderTabContent = () => {
    switch (activeTab) {
      case 'controller-services':
        return renderControllerServices();
      case 'reporting-tasks':
        return renderReportingTasks();
      case 'registry-clients':
        return (
          <div className="text-center py-12">
            <Server className="h-12 w-12 text-gray-400 mx-auto mb-4" />
            <h3 className="text-lg font-medium text-gray-900 mb-2">Registry Clients</h3>
            <p className="text-gray-500 mb-6">Manage connections to NiFi Registry instances.</p>
            <button className="btn-primary px-4 py-2 rounded-md">Add Registry Client</button>
          </div>
        );
      case 'parameters':
        return (
          <div className="text-center py-12">
            <SettingsIcon className="h-12 w-12 text-gray-400 mx-auto mb-4" />
            <h3 className="text-lg font-medium text-gray-900 mb-2">Parameter Providers</h3>
            <p className="text-gray-500 mb-6">Configure external parameter providers for dynamic configuration.</p>
            <button className="btn-primary px-4 py-2 rounded-md">Add Parameter Provider</button>
          </div>
        );
      default:
        return null;
    }
  };

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="bg-white border-b px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <SettingsIcon className="h-6 w-6 text-gray-500" />
            <h1 className="text-2xl font-bold text-gray-900">Settings</h1>
          </div>
          <button className="btn-outline px-4 py-2 rounded-md flex items-center space-x-2">
            <RefreshCw className="h-4 w-4" />
            <span>Refresh</span>
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div className="bg-white border-b">
        <div className="px-6">
          <nav className="flex space-x-8">
            {tabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`flex items-center space-x-2 py-4 px-1 border-b-2 font-medium text-sm ${
                  activeTab === tab.id
                    ? 'border-primary-500 text-primary-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                {tab.icon}
                <span>{tab.label}</span>
                {tab.count !== undefined && (
                  <span className="bg-gray-100 text-gray-600 py-0.5 px-2 rounded-full text-xs font-medium">
                    {tab.count}
                  </span>
                )}
              </button>
            ))}
          </nav>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 bg-gray-50 p-6 overflow-auto">
        {renderTabContent()}
      </div>
    </div>
  );
};

export default Settings;