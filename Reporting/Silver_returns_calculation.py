import os
import subprocess

from Utils.Common import get_file_path


def run_script(script_path):
    # Get the absolute path of the script
    abs_script_path = os.path.abspath(script_path)

    # Run the script and wait for it to complete
    result = subprocess.run(["python", abs_script_path], capture_output=True, text=True)

    # Check if the script ran successfully
    if result.returncode == 0:
        print(f"Successfully ran {abs_script_path}")
        return True
    else:
        print(f"Error running {abs_script_path}: {result.stdout}{result.stderr}")
        return False


# Path to the scripts
transform_script = get_file_path("S:/Users/THoang/Tech/LucidMA/Reporting/Bronze_tables/Bronze_returns_transform.py")
calculation_script = get_file_path("S:/Users/THoang/Tech/LucidMA/Reporting/Bronze_tables/Bronze_returns_calculation.py")

# Run the first script
if run_script(transform_script):
    # If the first script is successful, run the second script
    run_script(calculation_script)
