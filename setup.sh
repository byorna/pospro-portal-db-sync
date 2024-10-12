```bash
#!/bin/bash

# Update package list and install system dependencies
sudo apt update
sudo apt install -y msodbcsql17 unixodbc-dev

# Install Python dependencies
pip install -r requirements.txt