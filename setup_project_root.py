import os
import subprocess
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the project root from the environment variables
project_root = os.getenv('PROJECT_ROOT')

if not project_root:
    raise ValueError("PROJECT_ROOT not set in .env file")

# Define the command as a list of arguments
command = [f"export PYTHONPATH=$PYTHONPATH:{project_root}"]

# Run the command
result = subprocess.run(command, capture_output=True, text=True)

# Print the standard output and standard error
print("Standard Output:", result.stdout)
print("Standard Error:", result.stderr)

# Print the return code
print("Return Code:", result.returncode)

