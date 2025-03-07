# Optimizely Agent Notification Center

This solution provides a Python-based notification listener for Optimizely Agent that forwards notifications to Google Analytics and Amplitude.

## Overview

The solution consists of the following components:

1. **`main.py`** - Core notification listener that connects to Optimizely Agent's SSE (Server-Sent Events) endpoint and processes notifications.
2. **`google_analytics.py`** - Module for sending metrics to Google Analytics.
3. **`amplitude.py`** - Module for sending metrics to Amplitude.
4. **`Dockerfile`** - Container definition for running the notification center in a Kubernetes pod.
5. **`requirements.txt`** - Python dependencies required by the solution.

## Configuration

The application is configured using environment variables:

### Required Environment Variables

- `OPTIMIZELY_SDK_KEY` - Your Optimizely SDK key

### Optional Environment Variables

- `OPTIMIZELY_AGENT_URL` - URL of the Optimizely Agent (default: `http://localhost:8080`)
- `NOTIFICATION_FILTER` - Filter for specific notification types (e.g., `decision`)

### Google Analytics Configuration

- `GA_MEASUREMENT_ID` - Google Analytics measurement ID
- `GA_API_SECRET` - Google Analytics API secret
- `GA_DEBUG_MODE` - Set to "true" to enable debug mode (default: "false")

### Amplitude Configuration

- `AMPLITUDE_API_KEY` - Amplitude API key
- `AMPLITUDE_TRACKING_URL` - Amplitude tracking URL (default: `https://api2.amplitude.com/2/httpapi`)


## Running Optimizely Agent Locally for Development & Testing

Before running the notification listener, you need to have an instance of Optimizely Agent running. The easiest way to do this is using Docker:

```bash
docker run -d -p 8080:8080 -p 8085:8085 -p 8088:8088 -e OPTIMIZELY_LOG_PRETTY=true -e OPTIMIZELY_SERVER_HOST=0.0.0.0 -e OPTIMIZELY_SERVER_ALLOWEDHOSTS=localhost,127.0.0.1 --rm optimizely/agent
```

This command:
- Runs the Optimizely Agent container in detached mode
- Maps the necessary ports (8080, 8085, 8088) to your local machine
- Configures the agent to accept requests from localhost
- Automatically removes the container when it stops

You can verify the agent is running correctly by accessing the configuration endpoint:

```bash
curl -X GET "http://localhost:8080/v1/config" -H "X-Optimizely-SDK-Key: YOUR_SDK_KEY"
```

## Running Locally (Outside Container)

For local development, you can run the notification center directly on your machine using a virtual environment:

1. Create a virtual environment:

```bash
# On Windows
python -m venv venv

# On macOS/Linux
python3 -m venv venv
```

2. Activate the virtual environment:

```bash
# On Windows
venv\Scripts\activate

# On macOS/Linux
source venv/bin/activate
```

3. Install the required dependencies:

```bash
pip install -r requirements.txt
```

4. Copy the sample environment file to create your local configuration:

```bash
# On Windows
copy .env.sample .env

# On macOS/Linux
cp .env.sample .env
```

5. Edit the `.env` file with your actual configuration values.

6. Run the notification center:

```bash
python main.py
```

The script will automatically load environment variables from the `.env` file if it exists.

## Running in Docker

Build the container:

```bash
docker build -t optimizely-notification-center .
```

Run the container with proper environment variables:

```bash
docker run -d \
  --name notification-center \
  -e OPTIMIZELY_SDK_KEY=<your-sdk-key> \
  -e OPTIMIZELY_AGENT_URL=http://optimizely-agent:8080 \
  -e GA_MEASUREMENT_ID=<your-ga-id> \
  -e GA_API_SECRET=<your-ga-secret> \
  -e AMPLITUDE_API_KEY=<your-amplitude-key> \
  optimizely-notification-center
```

## Kubernetes Integration

The notification center is designed to run as a third container in the same pod as the PHP website and Optimizely Agent. Here's an example snippet to add to your existing Kubernetes pod configuration:

```yaml
- name: notification-center
  image: optimizely-notification-center:latest
  env:
    - name: OPTIMIZELY_SDK_KEY
      valueFrom:
        secretKeyRef:
          name: optimizely-secrets
          key: sdk-key
    - name: OPTIMIZELY_AGENT_URL
      value: "http://localhost:8080"  # Since running in same pod
    - name: GA_MEASUREMENT_ID
      valueFrom:
        secretKeyRef:
          name: analytics-secrets
          key: ga-id
    - name: GA_API_SECRET
      valueFrom:
        secretKeyRef:
          name: analytics-secrets
          key: ga-secret
    - name: AMPLITUDE_API_KEY
      valueFrom:
        secretKeyRef:
          name: analytics-secrets
          key: amplitude-key
```

## Data Flow

1. The PHP website sends requests to the Optimizely Agent for decisions and to track events.
2. The notification center listens to the Agent's SSE endpoint to receive notifications.
3. When a notification is received, it is processed and forwarded to:
   - Google Analytics using the Measurement Protocol
   - Amplitude using their HTTP API

## Security Considerations

- All API keys and secrets are passed via environment variables and should be stored securely.
- The container is based on the official Python 3.13 slim image for minimal attack surface.
- No sensitive data is logged in the application logs.

## Error Handling

The notification center includes robust error handling with:
- Automatic reconnection if the SSE connection is lost
- Detailed logging for troubleshooting
- Graceful handling of configuration errors

## Dependencies

- `sseclient-py`: For SSE connection
- `requests`: For HTTP requests to analytics platforms
- `urllib3`: For HTTP utilities

All dependencies are pinned to specific versions for stability.
