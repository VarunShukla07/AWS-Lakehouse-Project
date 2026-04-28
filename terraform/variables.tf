variable "db_password" {
  description = "Password for RDS PostgreSQL"
  type = string
  sensitive = true
}