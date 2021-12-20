import requests
from configparser import ConfigParser


def create_job_config(run_name, spark_version, node_type_id, notebook_path, packages):
  """
  Return a Databricks Job JSON configuration.
  """

  packages_json = [{"pypi": {"package": package}} for package in packages]

  cluster_config = {
                      "run_name": run_name,
                      "new_cluster": {
                          "spark_version": spark_version,
                          "node_type_id": node_type_id,
                          "num_workers": 0,
                          "spark_conf": {
                              "spark.master": "local[*]",
                              "spark.databricks.cluster.profile": "singleNode",
                              "spark.databricks.delta.preview.enabled": "true"
                              },
                      "custom_tags": {
                          "ResourceClass": "SingleNode"
                          },
                      "spark_env_vars": {
                          "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
                          "WSFS_ENABLE": "true"
                          },
                      },
                      "notebook_task" :{
                          "notebook_path": notebook_path
                      },
                      "libraries": packages_json
                      }

  return cluster_config


def get_credentials(databricks_config_path, environment):
    """Retrieve a pat token associated with a Databricks environment
    from the hidden Databricks CLI config file (.datbricks.cfg)

    Arguments:
        databricks_config_path: The path where the users .databrickscfg file is stored
        environment: The profile name specified in the .databrickscfg file. The workspace url and
                     PAT token will be sourced from this profile.
    """

    config = ConfigParser()
    config.read(f'{databricks_config_path}/.databrickscfg')

    workspace_url = config[environment]['host']
    token = config[environment]['token']
    
    return (workspace_url, token)


class JobRunner():
  """
  Run a Databricks Job via the runs-submit API
  """
  def __init__(self, job_config, workspace_url, pat_token):
      self.job_config = job_config
      self.workspace_url = workspace_url
      self.pat_token=pat_token
      self.headers = {
                    "Authorization":f'Bearer {self.pat_token}',
                    "Content-Type": "application/json"
                    }
      self.job_id=None
      self.jobs_api_url = f"{self.workspace_url}/api/2.0/jobs"


  def run(self):
    """
    https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-submit
    """
    endpoint = f'{self.jobs_api_url}/runs/submit'

    self.job_id = requests.post(endpoint, headers=self.headers, json=self.job_config).json()['run_id']

    return self.job_id


  @property
  def status(self):
    """
    Return the jobs status
    """

    if self.job_id:

      endpoint = f'{self.jobs_api_url}/runs/get?run_id={self.job_id}'

      response = requests.get(endpoint, headers=self.headers).json()

      return response['state']

    else:
      raise Exception('A job jas not been triggered')


  @property
  def results(self):
    """
    Retrieve all information about the job; including any output
    returned by the job
    """

    if self.job_id:

      endpoint = f'{self.jobs_api_url}/runs/get-output?run_id={self.job_id}'

      response = requests.get(endpoint, headers=self.headers).json()

      return response

    else:
      raise Exception('A job jas not been triggered')