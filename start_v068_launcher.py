import subprocess
import sys
import os

# Construct the full path to the script, handling potential special characters in the directory name
script_path = os.path.join('/Users/mac/QLIB', 'versions', 'v068', 'src', '机构级V13系统_v068_永久版.py')

# Verify the file exists before trying to run it
if not os.path.exists(script_path):
    print(f"Error: File not found at {script_path}")
    sys.exit(1)

# Get the path to the streamlit executable
streamlit_executable = os.path.join(os.path.dirname(sys.executable), 'streamlit')

# Command to run
command = [streamlit_executable, 'run', script_path]

print(f"Executing command: {' '.join(command)}")

# Run the streamlit application
subprocess.run(command)
