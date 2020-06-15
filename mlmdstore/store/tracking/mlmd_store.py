import json
import logging
import os
import sys
import shutil
import datetime


import uuid

from mlflow.entities import Experiment, Metric, Param, Run, RunData, RunInfo, RunStatus, RunTag, \
    ViewType, SourceType, ExperimentTag
from mlflow.entities.lifecycle_stage import LifecycleStage
from mlflow.entities.run_info import check_run_is_active, check_run_is_deleted
from mlflow.exceptions import MlflowException, MissingConfigException
import mlflow.protos.databricks_pb2 as databricks_pb2
from mlflow.models import Model
from mlflow.protos.databricks_pb2 import INTERNAL_ERROR, RESOURCE_DOES_NOT_EXIST
from mlflow.store.tracking import DEFAULT_LOCAL_FILE_AND_ARTIFACT_PATH, SEARCH_MAX_RESULTS_THRESHOLD
from mlflow.store.tracking.abstract_store import AbstractStore
#utils
from mlflow.utils.validation import _validate_metric_name, _validate_param_name, _validate_run_id, \
    _validate_tag_name, _validate_experiment_id, \
    _validate_batch_log_limits, _validate_batch_log_data
from mlflow.utils.env import get_env
from mlflow.utils.file_utils import (is_directory, list_subdirs, mkdir, exists, write_yaml,
                                     read_yaml, find, read_file_lines, read_file,
                                     write_to, append_to, make_containing_dirs, mv, get_parent_dir,
                                     list_all, local_file_uri_to_path, path_to_local_file_uri)
from mlflow.utils.search_utils import SearchUtils
from mlflow.utils.string_utils import is_string_type
from mlflow.utils.uri import append_to_uri_path
from mlflow.utils.mlflow_tags import MLFLOW_LOGGED_MODELS
#mlmd
from kubeflow.metadata import metadata




class MLMDStore(AbstractStore):
    #Constantes
    DEFAULT_EXPERIMENT_ID = "0"
    DATASET = 'dataset'
    MODEL = 'model'
    METRIC = 'metrics'
    METADATA_STORE_HOST_DEFAULT = "metadata-grpc-service.kubeflow"
    METADATA_STORE_PORT_DEFAULT = 8080


    #Constructor

    def __init__(self, metadata_store_host=METADATA_STORE_HOST_DEFAULT, metadata_store_port=METADATA_STORE_PORT_DEFAULT):
        """
        Create a new MLMDStore with the given mldm host and port
        """
        super(MLMDStore, self).__init__()
        # default workspace name = experiment default name
        self.metadata_store_host = metadata_store_host
        self.metadata_store_port = metadata_store_port



    #Funciones core del Abstract


    def list_experiments(self, view_type=ViewType.ACTIVE_ONLY):
        """

        :param view_type: Qualify requested type of experiments.

        :return: a list of Experiment objects stored in store for requested view.
        """
        pass


    def create_experiment(self, name, artifact_location=None):
        """
        Create a new experiment.
        If an experiment with the given name already exists, throws exception.

        :param name: Desired name for an experiment
        :param artifact_location: Base location for artifacts in runs. May be None.

        :return: experiment_id (string) for the newly created experiment if successful, else None.
        """
        #Creates a new workspace / Experiment proxy
        if ( name is None):
            name = Experiment.DEFAULT_EXPERIMENT_NAME
        self.mldm_workspace = self._get_or_create_workspace(name,self.metadata_store_host,self.metadata_store_port)
        return (Experiment.DEFAULT_EXPERIMENT_NAME)


    def get_experiment(self, experiment_id):
        """
        Fetch the experiment by ID from the backend store.

        :param experiment_id: String id for the experiment

        :return: A single :py:class:`mlflow.entities.Experiment` object if it exists,
            otherwise raises an exception.

        """
        pass

    def get_experiment_by_name(self, experiment_name):
        """
        Fetch the experiment by name from the backend store.
        This is a base implementation using ``list_experiments``, derived classes may have
        some specialized implementations.

        :param experiment_name: Name of experiment

        :return: A single :py:class:`mlflow.entities.Experiment` object if it exists.
        """
        for experiment in self.list_experiments(ViewType.ALL):
            if experiment.name == experiment_name:
                return experiment
        return None


    def delete_experiment(self, experiment_id):
        """
        Delete the experiment from the backend store. Deleted experiments can be restored until
        permanently deleted.

        :param experiment_id: String id for the experiment
        """
        pass


    def restore_experiment(self, experiment_id):
        """
        Restore deleted experiment unless it is permanently deleted.

        :param experiment_id: String id for the experiment
        """
        pass


    def rename_experiment(self, experiment_id, new_name):
        """
        Update an experiment's name. The new name must be unique.

        :param experiment_id: String id for the experiment
        """
        pass


    def get_run(self, run_id):
        """
        Fetch the run from backend store. The resulting :py:class:`Run <mlflow.entities.Run>`
        contains a collection of run metadata - :py:class:`RunInfo <mlflow.entities.RunInfo>`,
        as well as a collection of run parameters, tags, and metrics -
        :py:class`RunData <mlflow.entities.RunData>`. In the case where multiple metrics with the
        same key are logged for the run, the :py:class:`RunData <mlflow.entities.RunData>` contains
        the value at the latest timestamp for each metric. If there are multiple values with the
        latest timestamp for a given metric, the maximum of these values is returned.

        :param run_id: Unique identifier for the run.

        :return: A single :py:class:`mlflow.entities.Run` object, if the run exists. Otherwise,
                 raises an exception.
        """
        pass


    def update_run_info(self, run_id, run_status, end_time):
        """
        Update the metadata of the specified run.

        :return: :py:class:`mlflow.entities.RunInfo` describing the updated run.
        """
        pass


    def create_run(self, experiment_id, user_id, start_time, tags):
        """
        Create a run under the specified experiment ID, setting the run's status to "RUNNING"
        and the start time to the current time.

        :param experiment_id: String id of the experiment for this run
        :param user_id: ID of the user launching this run

        :return: The created Run object
        """
        experiment_id = MLMDStore.DEFAULT_EXPERIMENT_ID if experiment_id is None else experiment_id
        experiment = self.get_experiment(experiment_id)

        run_uuid = uuid.uuid4().hex
        run_info = RunInfo(run_uuid=run_uuid, run_id=run_uuid, experiment_id=experiment_id,
                           artifact_uri=None, user_id=user_id,
                           status=RunStatus.to_string(RunStatus.RUNNING),
                           start_time=start_time, end_time=None,
                           lifecycle_stage=LifecycleStage.ACTIVE)

        for tag in tags:
            self.set_tag(run_uuid, tag)
        self.mldm_run = self._get_or_create_workspace_run(experiment, run_uuid)
        return self.get_run(run_id=run_uuid)



    def delete_run(self, run_id):
        """
        Delete a run.

        :param run_id
        """
        pass


    def restore_run(self, run_id):
        """
        Restore a run.

        :param run_id
        """
        pass

    def log_metric(self, run_id, metric):
        _validate_run_id(run_id)
        _validate_metric_name(metric.key)
        run_info = self._get_run_info(run_id)
        check_run_is_active(run_info)
        self._log_run_metric(run_info, metric)


    def log_param(self, run_id, param):
        _validate_run_id(run_id)
        _validate_param_name(param.key)
        run_info = self._get_run_info(run_id)
        check_run_is_active(run_info)
        self._log_run_param(run_info, param)

    def set_experiment_tag(self, experiment_id, tag):
        """
        Set a tag for the specified experiment

        :param experiment_id: String id for the experiment
        :param tag: :py:class:`mlflow.entities.ExperimentTag` instance to set
        """
        pass

    def set_tag(self, run_id, tag):
        """
        Set a tag for the specified run

        :param run_id: String id for the run
        :param tag: :py:class:`mlflow.entities.RunTag` instance to set
        """
        self.log_batch(run_id, metrics=[], params=[], tags=[tag])


    def get_metric_history(self, run_id, metric_key):
        """
        Return a list of metric objects corresponding to all values logged for a given metric.

        :param run_id: Unique identifier for run
        :param metric_key: Metric name within the run

        :return: A list of :py:class:`mlflow.entities.Metric` entities if logged, else empty list
        """
        pass



    def _search_runs(self, experiment_ids, filter_string, run_view_type, max_results, order_by,
                     page_token):
        """
        Return runs that match the given list of search expressions within the experiments, as
        well as a pagination token (indicating where the next page should start). Subclasses of
        ``AbstractStore`` should implement this method to support pagination instead of
        ``search_runs``.

        See ``search_runs`` for parameter descriptions.

        :return: A tuple of ``runs`` and ``token`` where ``runs`` is a list of
            :py:class:`mlflow.entities.Run` objects that satisfy the search expressions,
            and ``token`` is the pagination token for the next page of results.
        """
        pass

    def list_run_infos(self, experiment_id, run_view_type):
        """
        Return run information for runs which belong to the experiment_id.

        :param experiment_id: The experiment id which to search

        :return: A list of :py:class:`mlflow.entities.RunInfo` objects that satisfy the
            search expressions
        """
        runs = self.search_runs([experiment_id], None, run_view_type)
        return [run.info for run in runs]


    def log_batch(self, run_id, metrics, params, tags):
        """
        Log multiple metrics, params, and tags for the specified run

        :param run_id: String id for the run
        :param metrics: List of :py:class:`mlflow.entities.Metric` instances to log
        :param params: List of :py:class:`mlflow.entities.Param` instances to log
        :param tags: List of :py:class:`mlflow.entities.RunTag` instances to log

        :return: None.
        """
        pass



    def record_logged_model(self, run_id, mlflow_model):
        """
        Record logged model information with tracking store. The list of logged model infos is
        maintained in a mlflow.models tag in JSON format.

        Note: The actual models are logged as artifacts via artifact repository.

        :param run_id: String id for the run
        :param mlflow_model: Model object to be recorded.

        NB: This API is experimental and may change in the future. The default implementation is a
        no-op.

        :return: None.
        """
        pass




    #Auxiliares

    #Crea un experimento (workspace)
    def _get_or_create_workspace(self,ws_name,metadata_store_host, metadata_store_port):
        return metadata.Workspace(
            store=metadata.Store(grpc_host=metadata_store_host, grpc_port=metadata_store_port),
            name=ws_name,
            description="CNN Text classifier metadata workspace",
            labels={"n1": "v1"})
    #Crea una ejecucion
    def _get_or_create_workspace_run(self,md_workspace, run_name):
        return metadata.Run(
            workspace=md_workspace,
            name=run_name,
            description="Metadata run for workflow %s" % run_name,
        )

    def _log_run_metric(self, run_info, metric):
        workspace_id = run_info.experiment_id
        run_id = run_info.run_id
        metric_type = metric.key
        metric_timestamp = metric.timestamp
        metric_value = metric.value

        exec = metadata.Execution(
            name=metric_type+ datetime.utcnow().isoformat("T"),
            workspace=workspace_id,
            run=run_id,
            description="Log metric: " + metric_type+ datetime.utcnow().isoformat("T")
        )
        metric_log = exec.log_input(
            metadata.Metrics(
                metrics_type=metric_type,
                values=metric_value
            ))



    def _log_run_param(self, run_info, param):
        workspace_id = run_info.experiment_id
        run_id = run_info.run_id
        param_type = param.key
        param_timestamp = param.timestamp
        param_value = param.value

        exec = metadata.Execution(
            name=param_type + datetime.utcnow().isoformat("T"),
            workspace=workspace_id,
            run=run_id,
            description="Log metric: " + param_type + datetime.utcnow().isoformat("T")
        )
        metric_log = exec.log_input(
            metadata.Metrics(
                metrics_type=param_type,
                values=param_value
            ))






