import sys
#from library.elastic import *
from library.rest_api import *
#from library.log import configure_collectd
from metrics import *
import os
#from library import graceful_exit
from multiprocessing.dummy import Pool as ThreadPool
#from library.kerberos_utils import *
from process_jhist import process_jhist
from library.hdfs_client import initialize_hdfs_client, copy_to_local


#collectd = logging.getLogger(__name__)


class Oozie:
    """Plugin object will be created only once and collects oozie statistics info every interval."""
    def __init__(self):
        """Initializes interval, oozie server, Job history and Timeline server details"""
        self.ooziehost = None
        self.oozieport = None
        self.timeline_server = None
        self.timeline_port = None
        self.job_history_server = None
        self.job_history_port = None
        self.resource_manager = None
        self.resource_manager_port = None
        self.interval = None
        self.with_threading = True
        self.workflows_processed = 0
        self.wfs_processed = 0
        self.error_wfs_processed = 0
        self.pool = None
        self.thread_count = 15
        self.url = None
        self.results = []
        self.use_rest_api = True


    def read_config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            elif children.key == OOZIEHOST:
                self.ooziehost = children.values[0]
            elif children.key == OOZIEPORT:
                self.oozieport = children.values[0]
            elif children.key == JOB_HISTORY_SERVER:
                self.job_history_server = children.values[0]
            elif children.key == JOB_HISTORY_PORT:
                self.job_history_port = children.values[0]
            elif children.key == RESOURCE_MANAGER:
                self.resource_manager = children.values[0]
            elif children.key == RESOURCE_MANAGER_PORT:
                self.resource_manager_port = children.values[0]
            elif children.key == TIMELINE_SERVER:
                self.timeline_server = children.values[0]
            elif children.key == TIMELINE_PORT:
                self.timeline_port = children.values[0]


    def get_redis_conn(self):
        """Function to connect redis database"""
        try:
            redis_obj = redis.Redis(host='localhost', port=6379, password=None)
            return redis_obj
        except:
            collectd.error("Plugin Oozie: Unable to connect redis") # pylint: disable=no-member
            return None

    def read_from_redis(self):
        """Function to read data from redis database"""
        redis_obj = self.get_redis_conn()
        if not redis_obj:
            return None
        if not redis_obj.get("workflows"):
            redis_obj.set("workflows", json.dumps({"workflows": []}))
        return json.loads(redis_obj.get("workflows"))

    def write_to_redis(self, workflow, index=-1):
        """Function to write data into redis"""
        workflows_data = self.read_from_redis()
        if not workflows_data:
            return
        if index == -1:
            workflows_data["workflows"].append(workflow)
        else:
            workflows_data["workflows"][index] = workflow
        redis_obj = self.get_redis_conn()
        redis_obj.set("workflows", json.dumps(workflows_data))


    def is_latest_oozie_job(self, lastjobdetails, latestjobdetails):
        """Function to check the Jobi details is already in flatMap file"""
        for workflow in lastjobdetails['workflows']:
            if workflow['wfId'] == latestjobdetails['wfId']:
                return False
        return True

    def change_workflow_status(self, workflow):
        """Function to change status of workflow in json file"""
        workflows = self.read_from_redis()
        if not workflows:
            return
        index = -1
        for i in range(0, len(workflows['workflows'])):
            if workflows['workflows'][i]['wfId'] == workflow['wfId']:
                index = i
                break
        if index != -1:
            self.write_to_redis(workflow, index)


    def read_unprocessed_workflows(self):
        """Function to process unprocessed workflows"""
        res_json = requests.get(self.url+'/jobs')
        if not res_json.ok:
            collectd.error("Unable to get oozie jobs from %s server and status is %s" \
                           %(self.ooziehost, res_json.status_code))
            return
        else:
            res_json = res_json.json()
        if not res_json['workflows']:
            return
        data = self.read_from_redis()
        if not data:
            return
        for workflow in res_json['workflows']:
            worklow_data = self.prepare_workflow(workflow)
            if not self.is_latest_oozie_job(data, worklow_data):
                continue
            self.write_to_redis(worklow_data)
        res_data = self.read_from_redis()
        if not res_data:
            return
        res_data = [workflow for workflow in res_data['workflows'] \
                    if workflow['workflowMonitorStatus'] != 'processed']
        if not res_data:
            return
        if self.with_threading:
            thread_pool = self.pool.map_async(self.process_workflow, res_data, callback=self.callback_wf_processed)
            thread_pool.wait()
        else:
            for workflow in res_data:
                self.process_workflow(workflow)


    def callback_wf_processed(self, status):
        collectd.debug("Callback called from async processing")
        collectd.info(status)
        self.wfs_processed += len(status)

    def _callback_error_wf_processed(self, status):
        collectd.debug("Callback called from error async processing")
        self.error_wfs_processed += 1


    def process_workflow(self, workflow):
        wf_process_start_time = time.time()
        url_format = "/oozie/v1/job/%s"
        url = url_format % (workflow['_source']['wfId'])
        workflowData = workflow['_source']
        collectd.info("WF {0} processing start time is {1}".format(workflow['_source']['wfId'], wf_process_start_time))
        result = False
        try:
            res_data = http_request(address=oozie['host'], port=oozie['port'], path=url, scheme=oozie['scheme'])
            if res_data:
            #collectd.debug("The oozie job detail received successfully for {0}, response: {1}".format( workflowData['wfId'], res_data))
                post_data = {"workflow" : workflow,
                             "workflowActions": [],
                             }
                if res_data['status'] == "SUCCEEDED" or res_data['status'] == "KILLED" or res_data['status'] == "FAILED":
                    for action in res_data['actions']:
                        actionData = {"action": None,
                                      "yarnJobs": []}
                    #Process Launcher job or if this is the only job with no childIDs
                        workflowActionData = prepare_workflow_action_data(action, 
                                                                      workflowData['wfId'], workflowData['wfName'])
                        actionData['action'] = workflowActionData
                        if action['externalId'] and action['externalId'] != '-':
                            yarnJobInfo = self.processYarnJob(action['externalId'], workflowData['wfId'], workflowData['wfName'],
                                                     action['id'], action['name'])
                            if yarnJobInfo:
                                actionData['yarnJobs'].append(yarnJobInfo)
                            else:
                                collectd.error("Don't have all info for wfaId %s" % action['id'])
                        if action['externalChildIDs']:
                            for externalChildID in action['externalChildIDs'].split(','):
                                yarnJobInfo = self.processYarnJob(externalChildID, workflowData['wfId'], workflowData['wfName'],action['id'], action['name'])
                                if yarnJobInfo:
                                    actionData['yarnJobs'].append(yarnJobInfo)
                                else:
                                    collectd.error("Don't have all info for wfaId %s" % action['id'])
                        if actionData['action']:
                            post_data["workflowActions"].append(actionData)
                    if post_data["workflowActions"]:
                        calculate_wf_metrics(res_data, post_data['workflowActions'])
                        try:
                            calculate_scheduling_delays_from_critical_path(res_data, post_data['workflowActions'])
                        except Exception as e:
                            collectd.exception("Failed to calculate delays from critical path")
                    #collectd.debug("Logging workflow details")
                    #collectd.debug(post_data)
                        for wfa in post_data["workflowActions"]:
                            if wfa['endTime'] == -1:
                                wfa['endTime'] = res_data['endTime']
                            self.build_collectd_document(wfa)
#                        result = send_bulk_workflow_to_elastic(res_data, post_data)
                elif res_data['status'] != "RUNNING":
                    collectd.debug("Current status: {0}, existing status:{1}".format(res_data['status'], workflowData['status']))
                    if res_data['status'] != workflowData['status']:
                        wf_data_to_send = prepare_workflow(res_data)
                        wf_data_to_send['status'] = res_data['status']
                        doc_data = {"doc": wf_data_to_send}
                        update_document_in_elastic(doc_data, workflow['_id'], indices['workflow'])
            else:
                collectd.error("Could not get workflow details for %s, received error from oozie server" % (workflowData['wfId']))
        except Exception as e:
            collectd.exception('Error Writing to ES => ' + traceback.format_exc().splitlines()[-1])
        finally:
            wf_process_end_time = time.time()
            collectd.info("Time taken to process wf {0} is {1}".format(workflow['_source']['wfId'], wf_process_end_time - wf_process_start_time))
            collectd.info("WF {0} processing end time is {1}".format(workflow['_source']['wfId'], wf_process_end_time))
            return result

    def build_collectd_document(self, wfa):
        for index in range(0, len(wfa["yarnJobs"])):
            for yarn_job in wfa["yarnJobs"][index]:
                if isinstance(wfa["yarnJobs"][index][yarn_job], dict):
                    self.results.append(wfa[yarn_job]['job'])
                elif isinstance(wfa["yarnJobs"][index][yarn_job], list):
                    self.results.extend(wfa[yarn_job]['job'])
            

    def compute_job_stats(self, job_info, app_info, task_info, tasks_map, tasks_reduce, oozieWorkflowId, oozieWorkflowActionId, oozieWorkflowName, oozieWorkflowActionName):
        yarnJobInfo = None
        hueristics = {}
        if job_info:
            job = job_info['jobId']
            job_info['waitTime'] = 0
            if tasks_reduce or tasks_map:
                job_info["waitTime"] = get_wait_time(job_info, tasks_reduce, tasks_map)
            if tasks_map:
                find_stragglers_runtime(tasks_map)
            if tasks_reduce:
                find_stragglers_runtime(tasks_reduce)
            tpTaskStats = calculate_taskcount_by_time_points(job_info, tasks_map + tasks_reduce, wfId=oozieWorkflowId,
                                                         wfaId=oozieWorkflowActionId, wfName=oozieWorkflowName,
                                                         wfaName=oozieWorkflowActionName)
        yarnJobInfo = {
            'job': job_info,
            'appInfo': app_info,
            'taskInfo': task_info,
            'taskAttemptsCounters': tasks_map + tasks_reduce,
            'tpTaskStats': tpTaskStats
        }
        return yarnJobInfo


    def processYarnJob(self, yarnJobId, oozieWorkflowId, oozieWorkflowName, oozieWorkflowActionId, oozieWorkflowActionName):
        collectd.debug("Processing yarnJobId %s of workflow %s workflowId: %s ActionId:%s ActionName:%s" % (
        yarnJobId, oozieWorkflowName, oozieWorkflowId,
        oozieWorkflowActionId, oozieWorkflowActionName))
        job = yarnJobId
        app = yarnJobId.replace("job_", "application_")
        tasks_map = []
        tasks_reduce = []
        task_info = None
        yarnJobInfo = None
        job_history_server = {"host": self.job_history_server, "port": self.job_history_port, "scheme":"http"}
        timeline_server = {"host":, self.timeline_server, "port": self.timeline_port, "scheme":"http"}
        job_info = get_job_info(job_history_server, job, oozieWorkflowName, oozieWorkflowId, oozieWorkflowActionId, oozieWorkflowActionName)
        if job_info:
            app_info = get_app_info(timeline_server, app, oozieWorkflowName, oozieWorkflowId, oozieWorkflowActionId, oozieWorkflowActionName)
            if use_rest_api:
                task_info = get_task_info(job_history_server, job, oozieWorkflowName, oozieWorkflowId, oozieWorkflowActionId,
                                          oozieWorkflowActionName)
                task_ids = get_task_ids_by_job(job_history_server, job)
                taskattempt_container_info = get_taskattempt_container_info(job_history_server, job, task_ids, oozieWorkflowName, oozieWorkflowId,
                                                                        oozieWorkflowActionId, oozieWorkflowActionName)
                if taskattempt_container_info:
                    for task in taskattempt_container_info:
                        for task_attempt in task:
                            if task_attempt['type'] == 'MAP':
                                tasks_map.append(task_attempt)
                            elif task_attempt['type'] == 'REDUCE':
                                tasks_reduce.append(task_attempt)
            else:
                jhist_file = copy_to_local(job_id=job, job_finish_time=job_info['finishTime'])
                if jhist_file and os.path.exists(jhist_file):
                    result = process_jhist(jhist_file, job_id=job, wfId=oozieWorkflowId, wfName=oozieWorkflowName,
                                           wfaId=oozieWorkflowActionId, wfaName=oozieWorkflowActionName)
                    task_info = result['taskInfo'].values()
                    tasks_map = result['tasksMap'].values()
                    tasks_reduce = result['tasksReduce'].values()
                else:
                    collectd.error("Job history file for job {0} does not exist in {1}".format(job, jhist_file))
            try:
                yarnJobInfo = self.compute_job_stats(job_info, app_info, task_info, tasks_map, tasks_reduce, oozieWorkflowId,
                                        oozieWorkflowActionId, oozieWorkflowName, oozieWorkflowActionName)
            except Exception as e:
                collectd.error("Failed to compute job stats for {0}".format(job))

        if yarnJobInfo: 
            return yarnJobInfo
        else:
            return None


    @staticmethod
    def add_common_params(oozie_dict, doc_type):
        """Adds TIMESTAMP, PLUGIN, PLUGIN_INS to dictionary."""
        hostname = gethostname()
        timestamp = int(round(time.time()))

        oozie_dict[HOSTNAME] = hostname
        oozie_dict[TIMESTAMP] = timestamp
        oozie_dict[PLUGIN] = 'oozie'
        oozie_dict[ACTUALPLUGINTYPE] = 'oozie'
        oozie_dict[PLUGINTYPE] = doc_type

    @staticmethod
    def dispatch_data(oozie_dict):
        """Dispatches dictionary to collectd."""
        collectd.info("Plugin Oozie: Values: %s" %(oozie_dict)) # pylint: disable=E1101
        dispatch(oozie_dict)

    def collect_data(self):
        """Collects all data."""
        if self.with_threading:
            self.pool = ThreadPool(self.thread_count)
        self.url = 'http://%s:%s/oozie/v1' %(self.ooziehost, self.oozieport)
        self.results = []
        self.read_unprocessed_workflows()
        for oozie_dict in self.results:
            if oozie_dict['_documentType'] == 'jobStats':
                oozie_dict = self.prepare_metrics(oozie_dict)
            self.add_common_params(oozie_dict, oozie_dict['_documentType'])
            self.dispatch_data(deepcopy(oozie_dict))

    def read(self):
        self.collect_data()

    def read_temp(self):
        """
        Collectd first calls register_read. At that time default interval is taken,
        hence temporary function is made to call, the read callback is unregistered
        and read() is called again with interval obtained from conf by register_config callback.
        """
        collectd.unregister_read(self.read_temp) # pylint: disable=E1101
        collectd.register_read(self.read, interval=int(self.interval)) # pylint: disable=E1101

oozieinstance = Oozie()
collectd.register_config(oozieinstance.read_config) # pylint: disable=E1101
collectd.register_read(oozieinstance.read_temp) # pylint: disable=E1101
