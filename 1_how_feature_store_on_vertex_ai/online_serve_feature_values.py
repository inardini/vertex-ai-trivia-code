from typing import List
from pandas import DataFrame
from google.cloud import aiplatform


def online_serve_feature_values(
        project: str,
        location: str,
        featurestore_id: str,
        entity_type_id: str,
        entity_ids: List[str],
        feature_ids: List[str]) -> DataFrame:
    """
    Retrieves online feature values from a Featurestore.
    Args:
        project: The Google Cloud project ID.
        location: The Google Cloud location.
        featurestore_id: The Featurestore ID.
        entity_type_id: The Entity Type ID.
        entity_ids: The list of Entity IDs.
        feature_ids: The list of Feature IDs.
    Returns:
        A Pandas DataFrame containing the feature values.
    """

    # Initialize the Vertex SDK for Python
    aiplatform.init(project=project, location=location)

    # Get the entity type from an existing Featurestore
    entity_type = aiplatform.featurestore.EntityType(entity_type_id=entity_type_id,
                                                     featurestore_id=featurestore_id)
    # Retrieve the feature values
    feature_values = entity_type.read(entity_ids=entity_ids, feature_ids=feature_ids)

    return feature_values
