# Unit Testing with unittest and Databricks Repos
Based on https://github.com/simondale/databricks-testing  

This repo contains an example workflow for implementing unit and integration tests on Databricks using Repos and the unittest Python framework.  

Tests are defined in **tests.py** and run via a Databricks notebook, **execute_tests**. The tests are executed remotely via the [Databricks Jobs API](https://docs.databricks.com/dev-tools/api/latest/jobs.html). Test results are returned in XML format that can be displayed in a CICD tool.

## Example:    

```
from test_runner import get_credentials, create_job_config, JobRunner
```

**Get credentials**  
These credentials are sourced from the hidden .databrickscfg file ceated as part of the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) configruation process. Credentials include the Workspace URL and PAT token. Since the .databrickscfg file can contain multiple 'profiles' - crendentials for different Workspaces - the profile name is also passed.
```
workspace_url, pat_token = get_credentials('<the directory path containing .databrickscfg file>', '<workspace profile>')
```

**Specify job parameters**  
```
run_name = "repos_tests"
spark_version = "10.1.x-scala2.12"
node_type_id = "i3.xlarge"
notebook_path = "/Repos/<project_or_user_name>/repos_test/tests/execute_tests"
packages = ["unittest-xml-reporting", "python-Levenshtein", "fuzzywuzzy"]
databricks_config_path = '/Users/user.name'
environment = 'default'
```

**Create a Databricks Job JSON configuration**
```
job_config = create_job_config(run_name, spark_version, node_type_id, notebook_path, packages)
```

**Instantiate a JobRunner and run the Databricks Job**
```
job = JobRunner(job_config, workspace_url, pat_token)
job_results = job.run_job()
```

**Get the xml formatted tests results for display in CICD tool**
```
print(job_results['notebook_output'])
```