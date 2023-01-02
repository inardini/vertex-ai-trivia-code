from typing import Dict, List
from pandas import DataFrame
from google.cloud import aiplatform


def batch_serve_features_df(
        project: str,
        location: str,
        featurestore_id: str,
        serving_feature_ids: Dict[str, List[str]],
        read_instances_df: DataFrame,
        pass_through_fields: List[str]) -> DataFrame:
    """
    Retrieves batch feature values from a Featurestore and writes them to a GCS bucket.
    Args:
        project: The Google Cloud project ID.
        location: The Google Cloud location.
        featurestore_id: The Featurestore ID.
        serving_feature_ids: The dictionary of Entity Type IDs and Feature IDs to retrieve.
        read_instances_df: The Pandas DataFrame containing entities and feature values.
        pass_through_fields: The list of fields to pass through extra to the label column.
    Returns:
        The Pandas DataFrame containing the dataset.
    """

    # Initialize the Vertex SDK for Python
    aiplatform.init(project=project, location=location)

    # Get an existing Featurestore
    featurestore = aiplatform.featurestore.Featurestore(featurestore_name=featurestore_id)

    # Get data with a point-in-time query from the Featurestore
    df = featurestore.batch_serve_to_df(
        serving_feature_ids=serving_feature_ids,
        read_instances_df=read_instances_df,
        pass_through_fields=pass_through_fields
    )

    return df


