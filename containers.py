from http_request import *
import json
import time
import collectd
from constants import *
from utils import *
from copy import deepcopy


class ContainrsStats:
    def __init__(self):
        self.cluster = None
        self.resource_manager_port = None

    def read_config(self, cfg):
        """Initializes variables from conf files."""
        for children in cfg.children:
            if children.key == INTERVAL:
                self.interval = children.values[0]
            elif children.key == CLUSTER:
                self.cluster = children.values[0]
            elif children.key == RESOURCE_MANAGER_PORT:
                self.resource_manager_port = children.values[0]

    def get_containers_node(self):

        location = self.cluster
        port = self.resource_manager_port
        path = '/ws/v1/cluster/nodes'
        nodes_json = http_request(location, port, path, scheme="http")
        if nodes_json is None:
            return None

        #collectd.debug("Node JSON:", json.dumps(nodes_json))
        nodes_list = nodes_json["nodes"]["node"]
        for node in nodes_list:
            node['time'] = int(round(time.time()))
#            node['_plugin'] = "yarn"
            node['_documentType'] = "containerStats"
#            node['_tag_appName'] = "hadoop"

        return nodes_list


    def get_cluster_metrics(self):
        location = self.cluster
        port = self.resource_manager_port
        path = '/ws/v1/cluster/metrics'
        metrics_json = http_request(location, port, path, scheme="http")
        if metrics_json is None:
            return None

        metrics_json['time'] = int(round(time.time()))
#        metrics_json['_plugin'] = "containers"
        metrics_json['_documentType'] = "clusterMetrics"
#        metrics_json['_tag_appName'] = "hadoop"

        #collectd.debug("Cluster metrics:", json.dumps(metrics_json))
        return metrics_json

    @staticmethod
    def add_common_params(namenode_dic, doc_type):
        """Adds TIMESTAMP, PLUGIN, PLUGIN_INS to dictionary."""
        hostname = gethostname()
        timestamp = int(round(time.time()))

        namenode_dic[HOSTNAME] = hostname
        namenode_dic[TIMESTAMP] = timestamp
        namenode_dic[PLUGIN] = 'containers'
        namenode_dic[ACTUALPLUGINTYPE] = 'containers'
        namenode_dic[PLUGINTYPE] = doc_type

    @staticmethod
    def dispatch_data(doc):
        """Dispatches dictionary to collectd."""
        collectd.info("Plugin container: Values: %s" %(doc)) # pylint: disable=E1101
        dispatch(doc)


    def collect_data(self):
        """Collects all data."""
        cluster_docs = self.get_cluster_metrics()
        containers_docs = self.get_containers_node()
        docs = []
        if cluster_docs:
            docs.append(cluster_docs)
        if containers_docs:
            docs.extend(containers_docs)
        for doc in docs:
            self.add_common_params(doc, doc['_documentType'])
            self.dispatch_data(deepcopy(doc))

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

containinstance = ContainrsStats()
collectd.register_config(containinstance.read_config) # pylint: disable=E1101
collectd.register_read(containinstance.read_temp) # pylint: disable=E1101
