# Products CLI

CLI tool for managing Products in AWS DynamoDB.

## Setup

### 1. Create and Activate Virtual Environment

```bash
cd /opt/mycode/shiv/cli

# Create virtual environment (if not already created)
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On Linux/Mac
# OR
venv\Scripts\activate     # On Windows
```

### 2. Install Dependencies

```bash
# Make sure virtual environment is activated (you should see (venv) in your prompt)
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. AWS Credentials

**Default Behavior:**
The script uses built-in AWS credentials by default. No configuration needed!

**Optional - Override with Environment Variables:**
If you want to use different credentials, set environment variables:
```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
```

**Optional - Use AWS Profile:**
If you want to use a specific AWS profile:
```bash
python3 products_cli.py your-profile-name
```

## Usage

### Option 1: Using Helper Script (Easiest)

```bash
cd /opt/mycode/shiv/cli
./run.sh
```

The helper script will automatically:
- Activate the virtual environment
- Run the CLI script
- Deactivate when done

### Option 2: Manual Activation

**Important:** Always activate the virtual environment before running the script!

```bash
cd /opt/mycode/shiv/cli
source venv/bin/activate  # Activate virtual environment
```

### Test Connection

```bash
# Using default credentials
python3 products_cli.py

# Using specific AWS profile
python3 products_cli.py your-profile-name
```

### Deactivate Virtual Environment

When you're done, deactivate the virtual environment:
```bash
deactivate
```

## Current Features

- ✅ Connect to DynamoDB Products table
- ✅ Test connection
- ✅ Display table information
- ✅ Create product with variants (interactive)

## Commands

### Test Connection
```bash
python3 products_cli.py
# OR
./run.sh
```

### Create Product with Variants
```bash
python3 products_cli.py --create
# OR
python3 products_cli.py -c
# OR
./run.sh --create
```

The create command will:
- Prompt for parent product information (name, category, subCategory, stockMode, etc.)
- Prompt for variant information (if any)
- Store in DynamoDB format (single item with `variants[]` array)
- Auto-generate product ID
- Calculate product status based on stock

## Next Steps

- List products
- Get product by ID
- Update product
- Delete product
- List variants for a product

