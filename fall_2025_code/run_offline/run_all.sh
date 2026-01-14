#!/bin/bash

# Run 1.py
echo "Running checkaffiliations2.py..."
python /home/mm4958/openalex/checkaffiliations2.py

# Check if 1.py succeeded
if [ $? -ne 0 ]; then
    echo "❌ checkaffiliations2.py failed. Exiting."
    exit 1
fi

# Run 2.pyu (assuming it's a typo and should be 2.py, or it's a valid script)
echo "Running get_years5.py"
python /home/mm4958/openalex/get_years5.py

# Check if 2.pyu succeeded
if [ $? -ne 0 ]; then
    echo "❌  get_years5.py failed."
    exit 1
fi

echo "✅ All scripts completed successfully."
