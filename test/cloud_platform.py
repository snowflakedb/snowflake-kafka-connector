from enum import Enum

# Enum for cloud platforms supported by Snowflake (AWS/AZURE/GCP)
class CloudPlatform(Enum):
    AWS = 1
    AZURE = 2
    GCP = 3
    ALL = 4
