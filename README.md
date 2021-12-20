# Unit Testing with unittest and Databricks Repos
Based on https://github.com/simondale/databricks-testing


## Example:    

```
from test_runner import get_credentials, create_job_config, JobRunner
```

**Get credentials**  
```
workspace_url, pat_token = get_credentials('<.databrickscfg directory>', '<workspace profile>')
```

**Specify job parameters**  
```
run_name = "repos_tests"
spark_version = "8.3.x-scala2.12"
node_type_id = "i3.xlarge"
notebook_path = "/Repos/<project_or_user_name>/repos_test/tests/execute_tests"
packages = ["unittest-xml-reporting", "python-Levenshtein", "fuzzywuzzy"]
```

**Create a Databricks Job JSON configuration**
```
job_config = create_job_config(run_name, spark_version, node_type_id, notebook_path, packages)
```

**Insantiate a JobRunner and run the Databricks Job**
```
job = JobRunner(job_config, workspace_url, pat_token)
job.run()
job.status
job.results
```