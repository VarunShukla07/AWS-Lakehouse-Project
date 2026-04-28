# Random suffix for unique bucket names
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# S3 Bucket for our data lake
resource "aws_s3_bucket" "lakehouse" {
  bucket = "varun-lakehouse-${random_id.bucket_suffix.hex}"
}

# Enable versioning (good practice)
resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

# VPC (Virtual Private Cloud) — isolated network for RDS
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
}

# Public subnet — where RDS will live (simplified for learning)
resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "us-east-1a"
  map_public_ip_on_launch = true
}

# Add this — second subnet in different AZ
resource "aws_subnet" "public_2" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = "us-east-1b"
  map_public_ip_on_launch = true
}

# Internet Gateway — so RDS can be reached from your laptop
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
}

# Route table — directs traffic
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
}

# Associate route table with subnet
resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# Security Group — firewall rules for RDS
resource "aws_security_group" "rds" {
  name_prefix = "rds-"
  vpc_id      = aws_vpc.main.id

  # Allow PostgreSQL access ONLY from your IP
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Allow all outbound traffic
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# DB Subnet Group — tells RDS which subnets to use
resource "aws_db_subnet_group" "main" {
  name       = "rds-subnet-group"
  subnet_ids = [aws_subnet.public.id, aws_subnet.public_2.id]  # ← TWO subnets
}

# Parameter Group — enables logical replication (needed for CDC)
resource "aws_db_parameter_group" "logical_replication" {
  family = "postgres15"
  name   = "logical-replication"

  parameter {
    name  = "rds.logical_replication"
    value = "1"
    apply_method = "pending-reboot" 
  }
}

# RDS PostgreSQL Instance
resource "aws_db_instance" "postgres" {
  identifier             = "ecommerce-db"
  engine                 = "postgres"
  engine_version         = "15"
  instance_class         = "db.t3.micro"      # FREE TIER ELIGIBLE
  allocated_storage      = 20
  storage_type           = "gp3"
  db_name                = "ecommerce"
  username               = "postgres"
  password               = var.db_password
  publicly_accessible    = true                # So you can connect from laptop
  vpc_security_group_ids = [aws_security_group.rds.id]
  db_subnet_group_name   = aws_db_subnet_group.main.name
  skip_final_snapshot    = true                # Don't keep backup when deleting
  parameter_group_name   = aws_db_parameter_group.logical_replication.name
}