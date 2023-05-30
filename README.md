S3 Features on cloudscale.ch
============================

Minimal setup to test S3 features on cloudscale.ch's Object storage.

## Usage

```shell
# Create virtualenv
python3 -m venv venv
source venv/activate

# Install dependencies
pip install -r requirements.txt

# Select 'rma' or 'lpg'
export CLOUDSCALE_REGION=rma

# Use an API token with write permissions
export CLOUDSCALE_API_TOKEN=...

# Run tests
pytest
```
