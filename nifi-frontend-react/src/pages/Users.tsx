import React, { useState } from 'react';
import { Users as UsersIcon, UserPlus, Search, Shield, Edit, Trash2 } from 'lucide-react';

interface User {
  id: string;
  identity: string;
  userGroups: string[];
  accessPolicies: number;
  configurable: boolean;
}

interface Group {
  id: string;
  identity: string;
  userCount: number;
  accessPolicies: number;
  configurable: boolean;
}

const Users: React.FC = () => {
  const [activeTab, setActiveTab] = useState('users');
  const [searchTerm, setSearchTerm] = useState('');

  const mockUsers: User[] = [
    {
      id: '1',
      identity: 'admin',
      userGroups: ['Administrators'],
      accessPolicies: 15,
      configurable: true
    },
    {
      id: '2',
      identity: 'analyst@company.com',
      userGroups: ['Data Analysts', 'Viewers'],
      accessPolicies: 5,
      configurable: true
    },
    {
      id: '3',
      identity: 'developer@company.com',
      userGroups: ['Developers'],
      accessPolicies: 8,
      configurable: true
    }
  ];

  const mockGroups: Group[] = [
    {
      id: '1',
      identity: 'Administrators',
      userCount: 1,
      accessPolicies: 25,
      configurable: true
    },
    {
      id: '2',
      identity: 'Data Analysts',
      userCount: 5,
      accessPolicies: 8,
      configurable: true
    },
    {
      id: '3',
      identity: 'Developers',
      userCount: 3,
      accessPolicies: 12,
      configurable: true
    },
    {
      id: '4',
      identity: 'Viewers',
      userCount: 8,
      accessPolicies: 3,
      configurable: true
    }
  ];

  const filteredUsers = mockUsers.filter(user =>
    user.identity.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const filteredGroups = mockGroups.filter(group =>
    group.identity.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const renderUsersTab = () => (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold">Users</h3>
        <button className="btn-primary px-4 py-2 rounded-md flex items-center space-x-2">
          <UserPlus className="h-4 w-4" />
          <span>Add User</span>
        </button>
      </div>

      <div className="card p-0 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Identity</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Groups</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Access Policies</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {filteredUsers.map((user) => (
                <tr key={user.id} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center">
                      <div className="h-8 w-8 bg-primary-600 rounded-full flex items-center justify-center text-white text-sm font-medium mr-3">
                        {user.identity.charAt(0).toUpperCase()}
                      </div>
                      <div className="font-medium text-gray-900">{user.identity}</div>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex flex-wrap gap-1">
                      {user.userGroups.map((group, index) => (
                        <span key={index} className="inline-flex px-2 py-1 text-xs font-semibold rounded-full bg-blue-100 text-blue-800">
                          {group}
                        </span>
                      ))}
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {user.accessPolicies}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    <div className="flex items-center space-x-2">
                      <button className="btn-outline px-2 py-1 rounded text-xs flex items-center space-x-1">
                        <Edit className="h-3 w-3" />
                        <span>Edit</span>
                      </button>
                      <button className="btn-outline px-2 py-1 rounded text-xs flex items-center space-x-1 text-red-600 border-red-300 hover:bg-red-50">
                        <Trash2 className="h-3 w-3" />
                        <span>Delete</span>
                      </button>
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

  const renderGroupsTab = () => (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold">Groups</h3>
        <button className="btn-primary px-4 py-2 rounded-md flex items-center space-x-2">
          <UserPlus className="h-4 w-4" />
          <span>Add Group</span>
        </button>
      </div>

      <div className="card p-0 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Identity</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Users</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Access Policies</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {filteredGroups.map((group) => (
                <tr key={group.id} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center">
                      <Shield className="h-5 w-5 text-gray-400 mr-3" />
                      <div className="font-medium text-gray-900">{group.identity}</div>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {group.userCount}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {group.accessPolicies}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    <div className="flex items-center space-x-2">
                      <button className="btn-outline px-2 py-1 rounded text-xs flex items-center space-x-1">
                        <Edit className="h-3 w-3" />
                        <span>Edit</span>
                      </button>
                      <button className="btn-outline px-2 py-1 rounded text-xs flex items-center space-x-1 text-red-600 border-red-300 hover:bg-red-50">
                        <Trash2 className="h-3 w-3" />
                        <span>Delete</span>
                      </button>
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
            <UsersIcon className="h-6 w-6 text-gray-500" />
            <h1 className="text-2xl font-bold text-gray-900">Users & Groups</h1>
          </div>
          <div className="relative max-w-md">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
            <input
              type="text"
              placeholder="Search users and groups..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="input pl-10"
            />
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="bg-white border-b">
        <div className="px-6">
          <nav className="flex space-x-8">
            <button
              onClick={() => setActiveTab('users')}
              className={`flex items-center space-x-2 py-4 px-1 border-b-2 font-medium text-sm ${
                activeTab === 'users'
                  ? 'border-primary-500 text-primary-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              <UsersIcon className="h-4 w-4" />
              <span>Users</span>
              <span className="bg-gray-100 text-gray-600 py-0.5 px-2 rounded-full text-xs font-medium">
                {mockUsers.length}
              </span>
            </button>
            <button
              onClick={() => setActiveTab('groups')}
              className={`flex items-center space-x-2 py-4 px-1 border-b-2 font-medium text-sm ${
                activeTab === 'groups'
                  ? 'border-primary-500 text-primary-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              <Shield className="h-4 w-4" />
              <span>Groups</span>
              <span className="bg-gray-100 text-gray-600 py-0.5 px-2 rounded-full text-xs font-medium">
                {mockGroups.length}
              </span>
            </button>
          </nav>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 bg-gray-50 p-6 overflow-auto">
        {activeTab === 'users' ? renderUsersTab() : renderGroupsTab()}
      </div>
    </div>
  );
};

export default Users;