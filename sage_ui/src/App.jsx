import { useState } from 'react'
import './App.css'
import TaskSubmission from './TaskSubmission'
import JobsList from './JobsList'

function App() {
  const [activeTab, setActiveTab] = useState(null)

  return (
    <div className="App">
      <header className="App-header">
        <h1>Sage UI</h1>
        <p>UI for Sage Server API</p>
      </header>
      
      <div className="tabs">
        <button 
          className={`tab-button ${activeTab === 'tasks' ? 'active' : ''}`}
          onClick={() => setActiveTab('tasks')}
        >
          Task Submission
        </button>
        <button 
          className={`tab-button ${activeTab === 'schedules' ? 'active' : ''}`}
          onClick={() => setActiveTab('schedules')}
        >
          Jobs
        </button>
      </div>

      <main>
        {!activeTab && (
          <div className="welcome-message">
            <h2>Welcome to Sage UI</h2>
            <p>Please select a tab above to get started.</p>
          </div>
        )}
        {activeTab === 'tasks' && <TaskSubmission />}
        {activeTab === 'schedules' && <JobsList />}
      </main>
    </div>
  )
}

export default App
