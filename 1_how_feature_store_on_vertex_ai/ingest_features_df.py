from typing import List
from pandas import DataFrame
from google.cloud import aiplatform


def ingest_features_df(
    project: str,
    location: str,
    featurestore_id: str,
    entity_type_id: str,
    features_ids: List[str],
    feature_time: str,
    features_df: DataFrame,
    entity_id_field: str
) -> aiplatform.featurestore.EntityType:
    """
    Ingests features into a Featurestore from a Pandas DataFrame.
    Args:
        project: The Google Cloud project ID.
        location: The Google Cloud location.
        featurestore_id: The Featurestore ID.
        entity_type_id: The Entity Type ID.
        features_ids: The list of Feature IDs.
        feature_time: The Feature timestamp.
        features_df: The Pandas DataFrame containing the features.
        entity_id_field: The Entity ID field.
    Returns:
        None
    """
    # Initialize the Vertex SDK for Python
    aiplatform.init(project=project, location=location)

    # Get the entity type from an existing Featurestore
    entity_type = aiplatform.featurestore.EntityType(entity_type_id=entity_type_id,
                                                     featurestore_id=featurestore_id)
    # Ingest the features
    entity_type.ingest_from_df(
        feature_ids=features_ids,
        feature_time=feature_time,
        df_source=features_df,
        entity_id_field=entity_id_field
    )

    return entity_type

