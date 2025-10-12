import React, { useState } from 'react';
import { Search, FileText, Clock, User, MapPin, Filter, Download, Eye } from 'lucide-react';

interface ProvenanceEvent {
  id: string;
  timestamp: string;
  eventType: string;
  componentType: string;
  componentName: string;
  filename: string;
  filesize: string;
  duration: string;
  user?: string;
}

const DataProvenance: React.FC = () => {
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedEventType, setSelectedEventType] = useState('all');
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');

  const eventTypes = [
    { value: 'all', label: 'All Events' },
    { value: 'RECEIVE', label: 'RECEIVE' },
    { value: 'CREATE', label: 'CREATE' },
    { value: 'SEND', label: 'SEND' },
    { value: 'FETCH', label: 'FETCH' },
    { value: 'DROP', label: 'DROP' },
    { value: 'ROUTE', label: 'ROUTE' },
    { value: 'MODIFY', label: 'MODIFY' },
  ];

  const mockProvenanceEvents: ProvenanceEvent[] = [
    {
      id: '1',
      timestamp: '2024-10-12 14:30:45.123',
      eventType: 'RECEIVE',
      componentType: 'GenerateFlowFile',
      componentName: 'Generate Sample Data',
      filename: 'data_001.json',
      filesize: '2.3 KB',
      duration: '12 ms',
      user: 'admin'
    },
    {
      id: '2',
      timestamp: '2024-10-12 14:30:45.135',
      eventType: 'ROUTE',
      componentType: 'RouteOnAttribute',
      componentName: 'Route by Type',
      filename: 'data_001.json',
      filesize: '2.3 KB',
      duration: '5 ms',
      user: 'admin'
    },
    {
      id: '3',
      timestamp: '2024-10-12 14:30:45.140',
      eventType: 'MODIFY',
      componentType: 'UpdateAttribute',
      componentName: 'Add Metadata',
      filename: 'data_001.json',
      filesize: '2.4 KB',
      duration: '8 ms',
      user: 'admin'
    },
    {
      id: '4',
      timestamp: '2024-10-12 14:30:45.148',
      eventType: 'SEND',
      componentType: 'PutFile',
      componentName: 'Write to Disk',
      filename: 'data_001.json',
      filesize: '2.4 KB',
      duration: '15 ms',
      user: 'admin'
    },
  ];

  const filteredEvents = mockProvenanceEvents.filter(event => {
    const matchesSearch = searchTerm === '' || 
      event.filename.toLowerCase().includes(searchTerm.toLowerCase()) ||
      event.componentName.toLowerCase().includes(searchTerm.toLowerCase());
    
    const matchesEventType = selectedEventType === 'all' || event.eventType === selectedEventType;
    
    return matchesSearch && matchesEventType;
  });

  const getEventTypeColor = (eventType: string) => {
    const colors: Record<string, string> = {
      'RECEIVE': 'bg-blue-100 text-blue-800',
      'CREATE': 'bg-green-100 text-green-800',
      'SEND': 'bg-purple-100 text-purple-800',
      'FETCH': 'bg-orange-100 text-orange-800',
      'DROP': 'bg-red-100 text-red-800',
      'ROUTE': 'bg-yellow-100 text-yellow-800',
      'MODIFY': 'bg-indigo-100 text-indigo-800',
    };
    return colors[eventType] || 'bg-gray-100 text-gray-800';
  };

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="bg-white border-b px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <FileText className="h-6 w-6 text-gray-500" />
            <h1 className="text-2xl font-bold text-gray-900">Data Provenance</h1>
          </div>
          <button className="btn-primary px-4 py-2 rounded-md flex items-center space-x-2">
            <Download className="h-4 w-4" />
            <span>Export Results</span>
          </button>
        </div>
      </div>

      {/* Search and Filters */}
      <div className="bg-white border-b px-6 py-4">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          {/* Search */}
          <div className="relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
            <input
              type="text"
              placeholder="Search by filename or component..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="input pl-10"
            />
          </div>

          {/* Event Type Filter */}
          <div>
            <select
              value={selectedEventType}
              onChange={(e) => setSelectedEventType(e.target.value)}
              className="input"
            >
              {eventTypes.map((type) => (
                <option key={type.value} value={type.value}>
                  {type.label}
                </option>
              ))}
            </select>
          </div>

          {/* Date Range */}
          <div>
            <input
              type="datetime-local"
              placeholder="Start Date"
              value={startDate}
              onChange={(e) => setStartDate(e.target.value)}
              className="input"
            />
          </div>
          <div>
            <input
              type="datetime-local"
              placeholder="End Date"
              value={endDate}
              onChange={(e) => setEndDate(e.target.value)}
              className="input"
            />
          </div>
        </div>

        <div className="flex items-center justify-between mt-4">
          <div className="flex items-center space-x-2 text-sm text-gray-600">
            <Filter className="h-4 w-4" />
            <span>Showing {filteredEvents.length} of {mockProvenanceEvents.length} events</span>
          </div>
          <button className="btn-outline px-3 py-1 rounded text-sm">
            Clear Filters
          </button>
        </div>
      </div>

      {/* Events Table */}
      <div className="flex-1 bg-gray-50 p-6 overflow-auto">
        <div className="card p-0 overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    <div className="flex items-center space-x-1">
                      <Clock className="h-4 w-4" />
                      <span>Timestamp</span>
                    </div>
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Event Type</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Component</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Filename</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Size</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Duration</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    <div className="flex items-center space-x-1">
                      <User className="h-4 w-4" />
                      <span>User</span>
                    </div>
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {filteredEvents.map((event) => (
                  <tr key={event.id} className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 font-mono">
                      {event.timestamp}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${getEventTypeColor(event.eventType)}`}>
                        {event.eventType}
                      </span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div>
                        <div className="font-medium text-gray-900">{event.componentName}</div>
                        <div className="text-sm text-gray-500">{event.componentType}</div>
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 font-mono">
                      {event.filename}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {event.filesize}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {event.duration}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {event.user || '-'}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      <div className="flex items-center space-x-2">
                        <button 
                          className="btn-outline px-2 py-1 rounded text-xs flex items-center space-x-1"
                          title="View Details"
                        >
                          <Eye className="h-3 w-3" />
                          <span>Details</span>
                        </button>
                        <button 
                          className="btn-outline px-2 py-1 rounded text-xs flex items-center space-x-1"
                          title="Show Lineage"
                        >
                          <MapPin className="h-3 w-3" />
                          <span>Lineage</span>
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Pagination */}
        <div className="flex items-center justify-between mt-6">
          <div className="text-sm text-gray-700">
            Showing page 1 of 1 (4 events total)
          </div>
          <div className="flex items-center space-x-2">
            <button className="btn-outline px-3 py-1 rounded text-sm" disabled>
              Previous
            </button>
            <button className="btn-outline px-3 py-1 rounded text-sm" disabled>
              Next
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DataProvenance;