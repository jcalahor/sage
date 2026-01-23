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

### Task Submission UI

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

### Example JSON Payload

```json
{
  "requestor_id": 10,
  "task_name": "PrimeTask",
  "task_envelope": "{\"limit\":2500}"
}
```

## Project Structure

```
sage_ui/
├── src/
│   ├── App.jsx              # Main application component
│   ├── App.css              # Application styles
│   ├── TaskSubmission.jsx   # Task submission form component
│   ├── TaskSubmission.css   # Task submission styles
│   ├── main.jsx             # Application entry point
│   └── index.css            # Global styles
├── public/                  # Static assets
├── index.html               # HTML template
└── vite.config.js           # Vite configuration
```

## Development

This is a React application built with Vite. It currently provides a task submission interface to interact with the Sage Server API.

### Prerequisites for Task Submission

Make sure the Sage Server is running at `http://localhost:4000` before submitting tasks through the UI.

### Next Steps

- Add task listing and status monitoring
- Implement scheduled task management interface
- Add task history and logs viewer
- Create dashboard views with metrics
