# Sage UI

UI application for the Sage Server API.

## Prerequisites

- Node.js 20.19+ or 22.12+ (required by Vite 7)
- npm 10+

## Setup

If you haven't already, install Node.js using nvm:

```bash
# Install nvm (if not already installed)
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash

# Load nvm
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"

# Install Node.js 20
nvm install 20
nvm use 20
```

Install dependencies:

```bash
cd sage_ui
npm install
```

## Running the Application

### Development Server

Start the development server with hot reload:

```bash
npm run dev
```

The application will be available at `http://localhost:5173`

### Build for Production

Create an optimized production build:

```bash
npm run build
```

The built files will be in the `dist/` directory.

### Preview Production Build

Preview the production build locally:

```bash
npm run preview
```

## Features

### Task Submission

The UI provides a user-friendly interface for submitting tasks to the Sage Server API:

- **Endpoint**: `POST http://localhost:4000/tasks/v1/start`
- **Free-form JSON input**: Edit and submit custom JSON payloads
- **Pre-populated example**: Comes with a working example JSON structure
- **Error handling**: Clear error messages for invalid JSON or API errors
- **Success responses**: Formatted display of API responses
- **Action buttons**:
  - **Submit Task**: Send the JSON payload to the API
  - **Reset to Example**: Restore the default example JSON
  - **Clear All**: Clear the input and responses

**Example JSON Payload:**
```json
{
  "requestor_id": 10,
  "task_name": "PrimeTask",
  "task_envelope": "{\"limit\":2500}",
  "priority": 0,
  "max_retries": 3
}
```

### Tasks List

View and monitor all submitted tasks with real-time status updates:

- **Task details**: View all task information including status, results, and errors
- **Status indicators**: Color-coded badges for pending, completed, and error states
- **Result viewing**: View task results as formatted JSON
- **Retry information**: See retry counts and maximum retry limits
- **Timestamps**: Track when tasks were created, started, and completed
- **Refresh**: Manually refresh the list to see latest status
- **Auto-refresh**: Optional automatic refresh every 5 seconds

### Jobs Management (Scheduled Tasks)

Create and manage cron-based scheduled tasks:

- **Job creation**: Create recurring tasks with cron expressions
- **Timezone support**: Schedule tasks in any timezone
- **Job list view**: See all scheduled jobs with next run times
- **Enable/disable**: Toggle jobs on or off without deleting them
- **Job history**: View execution history for each scheduled job
- **Status tracking**: Monitor job execution success/failure
- **Metadata support**: Add custom metadata to jobs

**Cron Expression Examples:**
- `0 2 * * *` - Daily at 2:00 AM
- `*/15 * * * *` - Every 15 minutes
- `0 9 * * 1-5` - Weekdays at 9:00 AM
- `0 0 1 * *` - First day of every month at midnight

## Project Structure

```
sage_ui/
├── src/
│   ├── App.jsx              # Main application with tab navigation
│   ├── App.css              # Application styles
│   ├── TaskSubmission.jsx   # Task submission form component
│   ├── TaskSubmission.css   # Task submission styles
│   ├── TasksList.jsx        # Task list and monitoring component
│   ├── TasksList.css        # Task list styles
│   ├── JobsList.jsx         # Jobs (scheduled tasks) management
│   ├── JobsList.css         # Jobs list styles
│   ├── main.jsx             # Application entry point
│   └── index.css            # Global styles
├── public/                  # Static assets
├── index.html               # HTML template
├── package.json             # Dependencies and scripts
└── vite.config.js           # Vite configuration
```

## Development

This is a React application built with Vite that provides a comprehensive interface for the Sage task queue system.

### Prerequisites

Make sure the Sage Server is running at `http://localhost:4000` before using the UI.

### Components

- **App.jsx**: Main component with tab-based navigation
- **TaskSubmission.jsx**: Form for submitting new tasks
- **TasksList.jsx**: View and monitor all tasks with status updates
- **JobsList.jsx**: Manage scheduled tasks (jobs) with cron expressions

### API Integration

The UI communicates with the Sage Server API:
- `POST /tasks/v1/start` - Submit new tasks
- `GET /tasks/v1/list` - Get all tasks
- `POST /jobs/v1/list` - Get all scheduled jobs
- `POST /jobs/v1/history` - Get job execution history

### Future Enhancements

- Real-time updates via WebSocket
- Task chains and workflow visualization
- Performance metrics and monitoring dashboard
- Advanced filtering and search capabilities
- Task cancellation functionality
