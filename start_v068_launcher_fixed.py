import subprocess
import sys
import os

# Path with the special character U+0010
correct_path = '/Users/mac/\u0010QLIB/versions/v068/src/机构级V13系统_v068_永久版.py'

# Verify the file exists
if not os.path.exists(correct_path):
    print(f"Error: File still not found at {correct_path}")
    # For debugging, let's see what's in the parent directory
    parent_dir = os.path.dirname(correct_path)
    if os.path.exists(parent_dir):
        print(f"Contents of {parent_dir}:")
        try:
            for item in os.listdir(parent_dir):
                print(f"- {item}")
        except Exception as e:
            print(f"Could not list directory: {e}")
    else:
        print(f"Parent directory not found: {parent_dir}")
    sys.exit(1)

# Find streamlit executable
streamlit_executable = os.path.join(os.path.dirname(sys.executable), 'streamlit')

# Command to run
command = [streamlit_executable, 'run', correct_path]

print(f"Executing command: {' '.join(command)}")

# Run the streamlit application
try:
    subprocess.run(command, check=True)
except subprocess.CalledProcessError as e:
    print(f"Streamlit command failed with exit code {e.returncode}")
    print(f"Stdout: {e.stdout}")
    print(f"Stderr: {e.stderr}")
    sys.exit(1)
except FileNotFoundError:
    print(f"Error: '{streamlit_executable}' not found. Is Streamlit installed in the current Python environment?")
    sys.exit(1)
