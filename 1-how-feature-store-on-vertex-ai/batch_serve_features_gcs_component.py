import os.path

from kfp.v2.dsl import component
from typing import Dict, List, NamedTuple

OUTPUT_COMPONENT_FILE = "batch_serve_features_gcs.yaml"
BASE_IMAGE = "python:3.9"
PACKAGES_TO_INSTALL = ["google-cloud-aiplatform"]


@component(output_component_file=OUTPUT_COMPONENT_FILE,
           base_image=BASE_IMAGE,
           packages_to_install=PACKAGES_TO_INSTALL)
def batch_serve_features_gcs(feature_store_id: str,
                             gcs_destination_output_uri_prefix: str,
                             gcs_destination_type: str,
                             serving_feature_ids: Dict[str, List[str]],
                             read_instances_uri: str,
                             project: str,
                             location: str) -> NamedTuple("Outputs", [("gcs_destination_output_uri_paths", str), ], ):
    # Import libraries
    import os
    from google.cloud import aiplatform
    from google.cloud.aiplatform.featurestore import Featurestore

    # Initialize Vertex AI client
    aiplatform.init(project=project, location=location)

    # Initiate feature store and run batch serve request
    featurestore = Featurestore(featurestore_name=feature_store_id)

    # Serve features in batch on GCS
    featurestore.batch_serve_to_gcs(
        gcs_destination_output_uri_prefix=gcs_destination_output_uri_prefix,
        gcs_destination_type=gcs_destination_type,
        serving_feature_ids=serving_feature_ids,
        read_instances_uri=read_instances_uri
    )

    # Store metadata
    gcs_destination_output_path_prefix = gcs_destination_output_uri_prefix.replace("gcs://", "/gcs/")
    gcs_destination_output_paths = os.path.join(gcs_destination_output_path_prefix, "*.csv")
    component_outputs = NamedTuple("Outputs",
                                   [("gcs_destination_output_uri_paths", str), ], )

    return component_outputs(gcs_destination_output_paths)
