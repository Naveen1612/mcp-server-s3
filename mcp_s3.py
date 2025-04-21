import boto3
from typing import Any
import httpx  # TODO remove this before committing
from rapidfuzz import process
from mcp.server.fastmcp import FastMCP

# Initialize FastMCP server
mcp = FastMCP("mcp_s3")

# Constants
AWS_PROFILE = "default"


def get_s3_objects(bucket: str, prefix: str) -> list:
    """
    Get a list of all objects in an S3 bucket with a specific prefix.

    Args:
        bucket (str): The name of the S3 bucket.
        prefix (str): The prefix to filter the objects.

    Returns:
        list: A list of object keys in the specified S3 bucket with the given prefix.
    """
    # Create an S3 client using the default profile
    session = boto3.Session(profile_name=AWS_PROFILE)
    s3_client = session.client("s3")

    # List objects in the specified bucket with the given prefix
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    # Extract and return the object keys from the response
    if "Contents" in response:
        return [obj["Key"] for obj in response["Contents"]]
    else:
        return []


def get_most_similar_file_names(file_names: list, match_string: str) -> list:
    """
    Get the most similar file names from a list based on a match string.

    Args:
        file_names (list): A list of file names.
        match_string (str): The string to match against.

    Returns:
        list: A list of the most similar file names.
    """
    # Use RapidFuzz to find the most similar file names
    matches = process.extract(match_string, file_names, limit=5)
    return [match[0] for match in matches]


@mcp.tool()
def get_similar_file_names(match_string: str) -> list:
    """
    Get the most similar file names from an S3 bucket based on a match string.

    Args:
        match_string (str): The string to match against.

    Returns:
        list: A list of the most similar file names.
    """
    # Get the list of objects in the S3 bucket with the specified prefix
    s3_objects = get_s3_objects("bwell-ingestion", "raw")

    # Get the most similar file names from the list of S3 objects
    similar_file_names = get_most_similar_file_names(s3_objects, match_string)

    return similar_file_names


if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')
