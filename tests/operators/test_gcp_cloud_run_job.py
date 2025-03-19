import inspect
from pathlib import Path
from unittest.mock import MagicMock, patch

import pkg_resources
import pytest
from airflow.utils.context import Context
from pendulum import datetime
#from collections import OrderedDict

try:
    from cosmos.operators.gcp_cloud_run_job import (
        DbtBuildGcpCloudRunJobOperator,
        DbtCloneGcpCloudRunJobOperator,
        DbtGcpCloudRunJobBaseOperator,
        DbtLSGcpCloudRunJobOperator,
        DbtRunGcpCloudRunJobOperator,
        DbtRunOperationGcpCloudRunJobOperator,
        DbtSeedGcpCloudRunJobOperator,
        DbtSnapshotGcpCloudRunJobOperator,
        DbtSourceGcpCloudRunJobOperator,
        DbtTestGcpCloudRunJobOperator,
    )

    class ConcreteDbtGcpCloudRunJobOperator(DbtGcpCloudRunJobBaseOperator):
        base_cmd = ["cmd"]

except (ImportError, AttributeError):
    DbtGcpCloudRunJobBaseOperator = None


BASE_KWARGS = {
    "task_id": "my-task",
    "project_id": "my-gcp-project-id",
    "region": "europe-west1",
    "job_name": "my-fantastic-dbt-job",
    "environment_variables": {"FOO": "BAR", "OTHER_FOO": "OTHER_BAR"},
    "project_dir": "my/dir",
    "vars": {
        "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
        "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
    },
    "no_version_check": True,
}


def skip_on_empty_operator(test_func):
    """
    Skip the test if DbtGcpCloudRunJob operators couldn't be imported.
    It is required as some tests don't rely on those operators and in this case we need to avoid throwing an exception.
    """
    return pytest.mark.skipif(
        DbtGcpCloudRunJobBaseOperator is None, reason="DbtGcpCloudRunJobBaseOperator could not be imported"
    )(test_func)


def test_overrides_missing():
    """
    The overrides parameter needed to pass the dbt command was added in apache-airflow-providers-google==10.11.0.
    We need to check if the parameter is actually present in required version.
    """
    required_version = "10.11.0"
    package_name = "apache-airflow-providers-google"

    from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

    installed_version = pkg_resources.get_distribution(package_name).version
    init_signature = inspect.signature(CloudRunExecuteJobOperator.__init__)

    if pkg_resources.parse_version(installed_version) < pkg_resources.parse_version(required_version):
        assert "overrides" not in init_signature.parameters
    else:
        assert "overrides" in init_signature.parameters


@skip_on_empty_operator
def test_dbt_gcp_cloud_run_job_operator_add_global_flags() -> None:
    """
    Check if global flags are added correctly.
    """
    dbt_base_operator = ConcreteDbtGcpCloudRunJobOperator(
        task_id="my-task",
        project_id="my-gcp-project-id",
        region="europe-west1",
        job_name="my-fantastic-dbt-job",
        project_dir="my/dir",
        vars={
            "start_time": "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
            "end_time": "{{ data_interval_end.strftime('%Y%m%d%H%M%S') }}",
        },
        no_version_check=True,
    )
    assert dbt_base_operator.add_global_flags() == [
        "--vars",
        "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
        "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
        "--no-version-check",
    ]


@skip_on_empty_operator
@patch("cosmos.operators.base.context_to_airflow_vars")
def test_dbt_gcp_cloud_run_job_operator_get_env(p_context_to_airflow_vars: MagicMock) -> None:
    """
    If an end user passes in a variable via the context that is also a global flag, validate that the both are kept
    """
    dbt_base_operator = ConcreteDbtGcpCloudRunJobOperator(
        task_id="my-task",
        project_id="my-gcp-project-id",
        region="europe-west1",
        job_name="my-fantastic-dbt-job",
        project_dir="my/dir",
    )
    dbt_base_operator.env = {
        "start_date": "20220101",
        "end_date": "20220102",
        "some_path": Path(__file__),
        "retries": 3,
        ("tuple", "key"): "some_value",
    }
    p_context_to_airflow_vars.return_value = {"START_DATE": "2023-02-15 12:30:00"}
    env = dbt_base_operator.get_env(
        Context(execution_date=datetime(2023, 2, 15, 12, 30)),
    )
    expected_env = {
        "start_date": "20220101",
        "end_date": "20220102",
        "some_path": Path(__file__),
        "START_DATE": "2023-02-15 12:30:00",
    }
    assert env == expected_env


@skip_on_empty_operator
@patch("cosmos.operators.base.context_to_airflow_vars")
def test_dbt_gcp_cloud_run_job_operator_check_environment_variables(
    p_context_to_airflow_vars: MagicMock,
) -> None:
    """
    If an end user passes in a variable via the context that is also a global flag, validate that the both are kept
    """
    dbt_base_operator = ConcreteDbtGcpCloudRunJobOperator(
        task_id="my-task",
        project_id="my-gcp-project-id",
        region="europe-west1",
        job_name="my-fantastic-dbt-job",
        project_dir="my/dir",
        environment_variables={"FOO": "BAR"},
    )
    dbt_base_operator.env = {
        "start_date": "20220101",
        "end_date": "20220102",
        "some_path": Path(__file__),
        "retries": 3,
        "FOO": "foo",
        ("tuple", "key"): "some_value",
    }
    expected_env = {"start_date": "20220101", "end_date": "20220102", "some_path": Path(__file__), "FOO": "BAR"}
    dbt_base_operator.build_command(context=MagicMock())

    assert dbt_base_operator.environment_variables == expected_env


@skip_on_empty_operator
def test_dbt_gcp_cloud_run_job_build_command():
    """
    Check whether the dbt command is built correctly.
    """

    result_map = {
        "ls": DbtLSGcpCloudRunJobOperator(**BASE_KWARGS),
        "run": DbtRunGcpCloudRunJobOperator(**BASE_KWARGS),
        "test": DbtTestGcpCloudRunJobOperator(**BASE_KWARGS),
        "seed": DbtSeedGcpCloudRunJobOperator(**BASE_KWARGS),
        "build": DbtBuildGcpCloudRunJobOperator(**BASE_KWARGS),
        "snapshot": DbtSnapshotGcpCloudRunJobOperator(**BASE_KWARGS),
        "source": DbtSourceGcpCloudRunJobOperator(**BASE_KWARGS),
        "clone": DbtCloneGcpCloudRunJobOperator(**BASE_KWARGS),
        "run-operation": DbtRunOperationGcpCloudRunJobOperator(macro_name="some-macro", **BASE_KWARGS),
    }

    for command_name, command_operator in result_map.items():
        command_operator.build_command(context=MagicMock(), cmd_flags=MagicMock())
        if command_name not in ("run-operation", "source"):
            assert command_operator.command == [
                "dbt",
                command_name,
                "--vars",
                "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
                "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
                "--no-version-check",
            ]
        elif command_name == "run-operation":
            assert command_operator.command == [
                "dbt",
                command_name,
                "some-macro",
                "--vars",
                "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
                "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
                "--no-version-check",
            ]
        else:
            assert command_operator.command == [
                "dbt",
                command_name,
                "freshness",
                "--vars",
                "end_time: '{{ data_interval_end.strftime(''%Y%m%d%H%M%S'') }}'\n"
                "start_time: '{{ data_interval_start.strftime(''%Y%m%d%H%M%S'') }}'\n",
                "--no-version-check",
            ]


@skip_on_empty_operator
def test_dbt_gcp_cloud_run_job_overrides_parameter():
    """
    Check whether overrides parameter passed on to CloudRunExecuteJobOperator is built correctly.
    """

    run_operator = DbtRunGcpCloudRunJobOperator(**BASE_KWARGS)
    run_operator.build_command(context=MagicMock(), cmd_flags=MagicMock())

    actual_overrides = run_operator.overrides

    assert "container_overrides" in actual_overrides
    actual_container_overrides = actual_overrides["container_overrides"][0]

    assert isinstance(actual_container_overrides["args"], list), "`args` should be of type list"

    assert "env" in actual_container_overrides
    actual_env = actual_container_overrides["env"]

    expected_env_vars = [{"name": "FOO", "value": "BAR"}, {"name": "OTHER_FOO", "value": "OTHER_BAR"}]

    for expected_env_var in expected_env_vars:
        assert expected_env_var in actual_env


@skip_on_empty_operator
@patch("cosmos.operators.gcp_cloud_run_job.CloudRunExecuteJobOperator.execute")
def test_dbt_gcp_cloud_run_job_build_and_run_cmd(mock_execute):
    """
    Check that building methods run correctly.
    """

    dbt_base_operator = ConcreteDbtGcpCloudRunJobOperator(
        task_id="my-task",
        project_id="my-gcp-project-id",
        region="europe-west1",
        job_name="my-fantastic-dbt-job",
        project_dir="my/dir",
        environment_variables={"FOO": "BAR"},
    )
    mock_build_command = MagicMock()
    dbt_base_operator.build_command = mock_build_command

    mock_context = MagicMock()
    dbt_base_operator.build_and_run_cmd(context=mock_context)

    mock_build_command.assert_called_with(mock_context, None)
    mock_execute.assert_called_once_with(dbt_base_operator, mock_context)


@skip_on_empty_operator
@patch("google.cloud.logging.Client.list_entries")
def test_dbt_gcp_cloud_run_job_fetch_clean_logs(mock_log_entries):
    """
    Check that fetch remote logs run correctly.
    """
    run_operator = DbtRunGcpCloudRunJobOperator(**BASE_KWARGS)
    #mock_list_entries.return_values = ['\x1b[0m10:41:16  Running with dbt=1.9.0', '\x1b[0m10:41:23  Registered adapter: bigquery=1.9.0', '\x1b[0m10:41:23  Unable to do partial parsing because a project config has changed', '\x1b[0m10:41:26  Found 1 model, 5 data tests, 2 sources, 491 macros', '\x1b[0m10:41:26  ', "\x1b[0m10:41:26  Concurrency: 3 threads (target='dev')", '\x1b[0m10:41:26  ', '\x1b[0m10:41:26  1 of 1 START freshness of ati.ati_events ....................................... [RUN]', '\x1b[0m10:41:29  1 of 1 WARN freshness of ati.ati_events ........................................ [\x1b[33mWARN\x1b[0m in 3.30s]', '\x1b[0m10:41:29  ', '\x1b[0m10:41:29  Finished running 1 source in 0 hours 0 minutes and 3.56 seconds (3.56s).', '\x1b[0m10:41:29  Done.', 'Container called exit(0).', OrderedDict({'@type': 'type.googleapis.com/google.cloud.audit.AuditLog', 'status': {'message': 'Execution data4all-dev-poc-airflow-bw8bs has completed successfully.'}, 'serviceName': 'run.googleapis.com', 'methodName': '/Jobs.RunJob', 'resourceName': 'namespaces/data4all-dev/executions/data4all-dev-poc-airflow-bw8bs', 'response': {'status': {'startTime': '2025-02-13T10:41:01.297597Z', 'logUri': 'https://console.cloud.google.com/logs/viewer?project=data4all-dev&advancedFilter=resource.type%3D%22cloud_run_job%22%0Aresource.labels.job_name%3D%22data4all-dev-poc-airflow%22%0Aresource.labels.location%3D%22europe-west1%22%0Alabels.%22run.googleapis.com/execution_name%22%3D%22data4all-dev-poc-airflow-bw8bs%22', 'completionTime': '2025-02-13T10:41:34.584198Z', 'conditions': [{'type': 'Completed', 'status': 'True', 'lastTransitionTime': '2025-02-13T10:41:34.584198Z'}, {'type': 'ResourcesAvailable', 'status': 'True', 'lastTransitionTime': '2025-02-13T10:40:58.916837Z'}, {'type': 'Started', 'status': 'True', 'lastTransitionTime': '2025-02-13T10:41:01.297597Z'}, {'type': 'ContainerReady', 'status': 'True', 'lastTransitionTime': '2025-02-13T10:40:58.758885Z'}, {'type': 'Retry', 'status': 'True', 'severity': 'Info', 'lastTransitionTime': '2025-02-13T10:41:35.987051Z', 'reason': 'ImmediateRetry', 'message': 'System will retry after 0:00:00 from lastTransitionTime for attempt 0.'}], 'observedGeneration': 1.0, 'succeededCount': 1.0}, 'metadata': {'uid': '7336760f-4560-4b61-8842-9c362eff9b78', 'ownerReferences': [{'controller': True, 'uid': '992f127c-a75b-4b66-9338-a184cec44c1a', 'blockOwnerDeletion': True, 'kind': 'Job', 'apiVersion': 'serving.knative.dev/v1', 'name': 'data4all-dev-poc-airflow'}], 'creationTimestamp': '2025-02-13T10:40:57.971264Z', 'namespace': '219597088506', 'generation': 1.0, 'annotations': {'run.googleapis.com/lastModifier': 'dbt-dev-sa@data4all-dev.iam.gserviceaccount.com', 'run.googleapis.com/client-name': 'cloud-console', 'run.googleapis.com/execution-environment': 'gen2', 'run.googleapis.com/creator': 'dbt-dev-sa@data4all-dev.iam.gserviceaccount.com', 'run.googleapis.com/operation-id': 'eb595ad0-eb2b-41d6-881f-d6d97943929f'}, 'labels': {'cloud.googleapis.com/location': 'europe-west1', 'run.googleapis.com/jobResourceVersion': '1739443241264970', 'run.googleapis.com/jobGeneration': '6', 'run.googleapis.com/job': 'data4all-dev-poc-airflow', 'run.googleapis.com/jobUid': '992f127c-a75b-4b66-9338-a184cec44c1a'}, 'resourceVersion': 'AAYuA7PDpWs', 'selfLink': '/apis/run.googleapis.com/v1/namespaces/219597088506/executions/data4all-dev-poc-airflow-bw8bs', 'name': 'data4all-dev-poc-airflow-bw8bs'}, 'kind': 'Execution', '@type': 'type.googleapis.com/google.cloud.run.v1.Execution', 'apiVersion': 'run.googleapis.com/v1', 'spec': {'template': {'spec': {'containers': [{'volumeMounts': [{'mountPath': '/secrets', 'name': 'secret-1'}], 'image': 'europe-west1-docker.pkg.dev/data4all-dev/data4all-preprod/dbt-poc-airflow@sha256:7022d95a9255fbb871c6e3e92852e8a983f2db539be90fb30c6599d4617969bb', 'env': [{'value': 'pilotage', 'name': 'AIRFLOW_CTX_DAG_OWNER'}, {'value': 'd4a_refresh_views_cloud_run_job', 'name': 'AIRFLOW_CTX_DAG_ID'}, {'value': 'ati_ati_events_source', 'name': 'AIRFLOW_CTX_TASK_ID'}, {'value': '2025-02-13T10:00:00+00:00', 'name': 'AIRFLOW_CTX_EXECUTION_DATE'}, {'value': '1', 'name': 'AIRFLOW_CTX_TRY_NUMBER'}, {'value': 'scheduled__2025-02-13T10:00:00+00:00', 'name': 'AIRFLOW_CTX_DAG_RUN_ID'}, {'value': 'data4all-dev', 'name': 'BIGQUERY_PROJECT_ID'}], 'args': ['dbt', 'source', 'freshness', '--select', 'source:ati.ati_events'], 'resources': {'limits': {'cpu': '1000m', 'memory': '512Mi'}}, 'name': 'test-materialization-table-1'}], 'timeoutSeconds': '600', 'serviceAccountName': 'dbt-dev-sa@data4all-dev.iam.gserviceaccount.com', 'volumes': [{'secret': {'secretName': 'dbt-service-keyfile', 'items': [{'key': 'latest', 'path': 'dbt-service-keyfile'}]}, 'name': 'secret-1'}], 'maxRetries': 3.0}}, 'parallelism': 1.0, 'taskCount': 1.0}}})]
    mock_list_entries = {
        "latest_created_execution": {
            "name":"test-job-cloud-run",
            "create_time":"2025-03-19T09:41:22.608063Z"
        }
    }
    log_messages = run_operator.fetch_remote_logs(mock_list_entries)
    
    assert len(log_messages) > 0
