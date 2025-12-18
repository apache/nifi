# S3 Configuration

This step configures the connection to Amazon S3 or an S3-compatible storage system.

## S3 Destination Configuration

### S3 Bucket

The name of the S3 bucket where data will be written. Ensure the bucket exists
and your credentials have write permissions.

### S3 Prefix

An optional prefix (folder path) to prepend to all object keys. For example,
setting this to `kafka-data/` will result in objects like:
`kafka-data/2024/01/15/data-001.json`

### S3 Region

The AWS region where your S3 bucket is located (e.g., `us-east-1`, `eu-west-1`).

### S3 Data Format

The format to use when writing objects to S3:

- **JSON**: Write data as JSON objects (human-readable)
- **Avro**: Write data in Apache Avro format (compact, schema-embedded)

### S3 Endpoint Override URL

Leave this blank for standard AWS S3. Use this field only when connecting to
S3-compatible storage systems like MinIO, LocalStack, or Ceph.

## Merge Configuration

### Target Object Size

The connector merges multiple Kafka messages together before writing to S3 to
reduce the number of objects created. Specify the target size for merged objects
(e.g., `256 MB`, `1 GB`).

### Merge Latency

The maximum time to wait while collecting messages before writing to S3. Even if
the target size hasn't been reached, data will be written after this duration to
ensure timely data availability.

## S3 Credentials

### Authentication Strategy

Choose how to authenticate with AWS:

| Strategy | Description |
|----------|-------------|
| Access Key ID and Secret Key | Use explicit AWS access credentials |
| Default AWS Credentials | Use the default credential chain (environment variables, IAM roles, etc.) |

### S3 Access Key ID / Secret Access Key

When using explicit credentials, provide your AWS access key ID and secret access key.

**Security Note:** Store sensitive credentials using NiFi's sensitive property
protection or parameter contexts with secret providers.

