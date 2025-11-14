# AWS DynamoDB Tables Setup Guide

This guide explains which DynamoDB tables you need to create in AWS for the CLI to work properly.

## Required Tables

### 1. **Pincodes** Table (REQUIRED)

**Table Name:** `Pincodes`

**Primary Key:**
- **Partition Key:** `pincodeID` (String)

**Table Settings:**
- **Billing Mode:** On-Demand (recommended) or Provisioned
- **Region:** ap-south-1 (or your preferred region)

**Attributes Stored:**
- `pincodeID` (String) - Primary key, format: PIN-0001, PIN-0002, etc.
- `id` (String) - Same as pincodeID
- `pincodeNumber` (String) - The actual pincode number (e.g., "560001")
- `status` (String) - "Active" or "Inactive"
- `deliveryTypes` (List of Strings) - Array of delivery type names
- `areas` (List of Maps) - Array of area objects with:
  - `id` (String)
  - `name` (String)
  - `pincodeId` (String)
  - `createdAt` (String)
- `assignedShifts` (List of Maps) - Array of delivery shift objects
- `charges` (List of Maps) - Array of charge objects with:
  - `id` (String)
  - `pincodeId` (String)
  - `minOrderValue` (Number/Decimal)
  - `maxOrderValue` (Number/Decimal, optional)
  - `charge` (Number/Decimal)
  - `isActive` (Boolean)
  - `createdAt` (String)
  - `updatedAt` (String)
- `createdAt` (String) - ISO timestamp
- `updatedAt` (String) - ISO timestamp

**How to Create in AWS Console:**
1. Go to DynamoDB Console → Tables → Create table
2. Table name: `Pincodes`
3. Partition key: `pincodeID` (String)
4. Leave sort key empty
5. Table settings: Use default settings (On-Demand billing mode recommended)
6. Click "Create table"

---

### 2. **Delivery_types** Table (OPTIONAL)

**Table Name:** `Delivery_types`

**Primary Key:**
- **Partition Key:** `id` (String) or `deliveryType` (String)

**Note:** This table is optional. If it doesn't exist, the CLI will still work but you won't be able to assign delivery types to pincodes during creation.

**Attributes Stored:**
- `id` (String) - Unique identifier
- `deliveryType` (String) - e.g., "Same Day Delivery", "Next Day Delivery"
- `isActive` (Boolean)
- `createdAt` (String)
- `updatedAt` (String)
- `slots` (List of Maps) - Array of time slot objects (optional)

**How to Create in AWS Console:**
1. Go to DynamoDB Console → Tables → Create table
2. Table name: `Delivery_types`
3. Partition key: `id` (String) or `deliveryType` (String)
4. Leave sort key empty
5. Table settings: Use default settings
6. Click "Create table"

---

## Summary

### Minimum Required:
- ✅ **Pincodes** table (REQUIRED)

### Optional (for full functionality):
- ⚠️ **Delivery_types** table (OPTIONAL - for delivery type assignment)

---

## Quick Setup Steps

1. **Log in to AWS Console**
   - Go to https://console.aws.amazon.com/dynamodb/

2. **Create Pincodes Table:**
   ```
   Table name: Pincodes
   Partition key: pincodeID (String)
   Billing mode: On-Demand
   ```

3. **Create Delivery_types Table (Optional):**
   ```
   Table name: Delivery_types
   Partition key: id (String)
   Billing mode: On-Demand
   ```

4. **Verify Tables:**
   - Both tables should appear in your DynamoDB tables list
   - Status should be "Active"

5. **Test Connection:**
   - Run your CLI script
   - It should connect to the tables automatically
   - If tables don't exist, you'll see a warning message

---

## Important Notes

- **Region:** Make sure your tables are created in the same AWS region as your CLI script (default: `ap-south-1`)
- **IAM Permissions:** Your AWS credentials need the following DynamoDB permissions:
  - `dynamodb:PutItem`
  - `dynamodb:GetItem`
  - `dynamodb:UpdateItem`
  - `dynamodb:DeleteItem`
  - `dynamodb:Scan`
  - `dynamodb:Query`
  - `dynamodb:DescribeTable`
- **Primary Key:** The `pincodeID` must be unique for each pincode
- **Data Types:** DynamoDB will automatically handle the data types (String, Number, List, Map, Boolean)

---

## Example IAM Policy (if needed)

If you need to create an IAM policy for DynamoDB access, here's a minimal policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:PutItem",
                "dynamodb:GetItem",
                "dynamodb:UpdateItem",
                "dynamodb:DeleteItem",
                "dynamodb:Scan",
                "dynamodb:Query",
                "dynamodb:DescribeTable"
            ],
            "Resource": [
                "arn:aws:dynamodb:ap-south-1:*:table/Pincodes",
                "arn:aws:dynamodb:ap-south-1:*:table/Delivery_types"
            ]
        }
    ]
}
```

---

## Troubleshooting

**Issue:** CLI shows "Pincodes table not found"
- **Solution:** Create the `Pincodes` table in DynamoDB with partition key `pincodeID` (String)

**Issue:** Can't assign delivery types
- **Solution:** Create the `Delivery_types` table (optional, but needed for delivery type features)

**Issue:** Access denied errors
- **Solution:** Check your AWS credentials have DynamoDB permissions

**Issue:** Tables in different region
- **Solution:** Either create tables in `ap-south-1` or update the region in your CLI script

