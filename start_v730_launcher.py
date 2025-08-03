import subprocess
import os
import sys

# Path with the special character
file_path = "/Users/mac/\u0010QLIB/机构级V13系统_v730_改进版.py"

print(f"Attempting to run: {file_path}")

if not os.path.exists(file_path):
    print(f"Error: File not found at {file_path}")
    dir_path = os.path.dirname(file_path)
    print(f"Checking directory: {dir_path}")
    if os.path.exists(dir_path):
        print("Directory contents:")
        try:
            for item in os.listdir(dir_path):
                print(f"- {item}")
        except Exception as e:
            print(f"Could not list directory contents: {e}")
    else:
        print("Directory does not exist.")
    sys.exit(1)

# Command to run
command = ["streamlit", "run", file_path]

print(f"Executing command: {' '.join(command)}")

try:
    # Using subprocess.run to execute the command
    subprocess.run(command, check=True)
except subprocess.CalledProcessError as e:
    print(f"Error running streamlit: {e}")
    sys.exit(1)
except FileNotFoundError:
    print("Error: 'streamlit' command not found. Make sure it's installed and in your PATH.")
    sys.exit(1)
