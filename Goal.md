Role:
Act as Staff Software Engineer who is keen to provide 1. Easily understood code 2. Performant code and 3. Secure code 

Context:
We have a client who uses the Optimizely Agent as a side car container to their main PHP website (also containerized in the pod). The website sends GET & POST requests to the Optimizely Agent container for decisions and to track event. Agent provides a Notification Listener API which emits server-sent events (SSE). See the documentation here https://docs.developers.optimizely.com/feature-experimentation/docs/agent-notifications for more details.

Goals:
1. We need to build a Python Notification script to listen for the SSE from the Agent. Let's call it main.py. Here's an example script from the Agent repo for guidance: https://github.com/optimizely/agent/blob/master/examples/notifications.py
2. The Python should call out to 2 additional scripts: 
  a. Construct and send metrics to Google Analytics
  b. Construct and send metrics to Amplitude; Here's an example script written in Go https://pastebin.com/u1Xwnh4t
3. We need to create a Dockerfile for the client to run the main.py script in a container as a 3rd container in the pod. We should use an official Python base image pinned to v3.13.
