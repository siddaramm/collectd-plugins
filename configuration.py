reduce_start = 70.0
out_file = "./var/log/hueristics/hadoop-plugin-test"

logging_config = {
    "ozzieWorkflows" : "./var/log/processOzzieWorkflows.log",
    "elasticWorkflows" : "./var/log/processElasticWorkflows.log",
    "sparkJobs": "./var/log/sparkJobs.log",
    "hadoopCluster": "./var/log/hadoopClusterStats.log"
}

previous_json_yarn = './var/previous_json_yarn'
previous_json_nn = './var/previous_json_nn'


sleep_time_in_secs = {
    "oozieWorkflows": 30,
    "elasticWorkFlows": 30
}

tasks_by_time = {
    "numOfDataPoints": 25,
    "minimumInterval": 5  # seconds
}

read_timeout = None
write_timeout = None

kerberos = {
    "enabled" : False,
    "principal" : "",
    "keytab_file": ""
}

hdfs_jobhistory_directory = "/mr-history/done"
jobhistory_copy_dir = "./var/jhist"
hueristics_out_dir = "./var/hueristics"
use_rest_api = False


modify_user_agent_header = True


cluster_timezones = {
    'EST': "US/Eastern",
    "EDT": "US/Eastern",
    "CST": "US/Central",
    "CDT": "US/Central",
    "PST": "US/Pacific",
    "PDT": "US/Pacific"
}

time_before_in_seconds = 30000
yarn_stats_time_interval = 30
container_stats_time_interval = 120
name_node_stats_time_interval = 120
job = "job_1535683071103_24598"
app = job.replace("job", "application")

tasks_by_time = {
    'numOfDataPoints': 25,
    'minimumInterval': 5
}
