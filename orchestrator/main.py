import base64
import json
import logging
import os
import functions_framework
from google.cloud import run_v2
from google.cloud.run_v2.types.job import RunJobRequest
from google.cloud.run_v2.types.k8s_min import EnvVar

ANALYSIS_PROJECT = os.environ.get("ANALYSIS_PROJECT")
REGION = os.environ.get("REGION", "us-east4")

if not ANALYSIS_PROJECT:
    raise RuntimeError("ANALYSIS_PROJECT env var is not set")

# Explicit allowlist — maps GCS path to Cloud Run Job name.
# Anything not listed here is silently ignored.
# tennis/players.json is intentionally excluded — WTA classifier not yet wired.
ROUTES = {
    "cbb/kenpom.json":  "cbb-processor",
    "cbb/odds.json":    "cbb-processor",
    "tennis/odds.json": "tennis-processor",
}


@functions_framework.cloud_event
def handler(cloud_event):
    message_data = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
    data = json.loads(message_data)

    message_id = cloud_event.data["message"]["messageId"]
    file_path  = data.get("name")
    bucket     = data.get("bucket")
    generation = data.get("generation")

    job_name = ROUTES.get(file_path)

    if not job_name:
        logging.info(f"[{message_id}] Ignored: {file_path}")
        return

    job_path = f"projects/{ANALYSIS_PROJECT}/locations/{REGION}/jobs/{job_name}"
    logging.info(f"[{message_id}] Triggering {job_name} for {file_path} gen={generation}")

    # Build overrides using SDK proto types — plain dicts are silently dropped
    # by run_job() and env vars never reach the container.
    Overrides         = RunJobRequest.Overrides
    ContainerOverride = Overrides.ContainerOverride

    request = RunJobRequest(
        name=job_path,
        overrides=Overrides(
            container_overrides=[
                ContainerOverride(
                    env=[
                        EnvVar(name="TRIGGER_GCS_BUCKET", value=bucket        or ""),
                        EnvVar(name="TRIGGER_GCS_PATH",   value=file_path     or ""),
                        EnvVar(name="TRIGGER_GCS_GEN",    value=str(generation or "")),
                        EnvVar(name="TRIGGER_MESSAGE_ID", value=message_id    or ""),
                    ]
                )
            ]
        ),
    )

    client = run_v2.JobsClient()
    try:
        operation = client.run_job(request=request)
        logging.info(f"[{message_id}] Job {job_name} dispatched. operation={operation.operation.name}")
    except Exception as e:
        logging.error(f"[{message_id}] Failed to dispatch {job_name}: {e}")