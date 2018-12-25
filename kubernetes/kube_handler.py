# -*- coding: utf-8 -*-
from kubernetes import client, config
from kubernetes.client import ApiClient
from kubernetes.stream import stream
import logging
import select
import six
import errno
import os
import sys
from copy import deepcopy

logger = logging.getLogger("k8s handler")
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.formatter = formatter
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

COMMAND = ["/bin/sh", "-c",
           "ps -aux|awk '{print$3\" \"$4\" \"$11}';nvidia-smi | grep % |awk '{print$9\" \"$11\" \"$13}'"]


class K8sHandler(object):
    def __init__(self):
        self.load_k8s_cfg()
        self.k8s_batch = client.BatchV1Api()
        self.k8s_coreapi = client.CoreV1Api()
        self.api_client = ApiClient()
        self.allocatable_resources = self.allocatable_resources()

    def load_k8s_cfg(self, cfg_file=None):
        try:
            config.load_kube_config(cfg_file)
        except Exception as e:
            logger.error('load kube config error %s' % e)
            try:
                config.load_incluster_config()
                logger.info('load cluster config succeed')
            except Exception as e:
                logger.error('load in cluster config error %s' % e)
                return False
        return True

    def exec(self, name, namespace, exec_command=''):
        sh_cmd = [
            '/bin/sh',
            '-c',
            exec_command
        ]
        resp = stream(self.k8s_coreapi.connect_get_namespaced_pod_exec, name, namespace,
                      command=sh_cmd,
                      stderr=True, stdin=False,
                      stdout=True, tty=False)
        return resp

    def exec_stream(self, name, namespace):
        exec_command = ['/bin/sh']
        resp = stream(self.k8s_coreapi.connect_get_namespaced_pod_exec, name, namespace,
                      command=exec_command,
                      stderr=True, stdin=True,
                      stdout=True, tty=True,
                      _preload_content=False)
        return resp

    def exec_interactive(self, name, namespace):
        exec_command = ['/bin/sh']
        resp = stream(self.k8s_coreapi.connect_get_namespaced_pod_exec, name, namespace,
                      command=exec_command,
                      stderr=True, stdin=True,
                      stdout=True, tty=True,
                      _preload_content=False)

        k8s_stream = resp
        input_source = 0
        while resp.is_open():
            try:
                rfds, wfds, xfds = select.select([input_source,
                                                  k8s_stream.sock.sock],
                                                 [], [])
            except select.error as e:
                logger.error('select setup error: %s' % e)
                no = e.errno if six.PY3 else e[0]
                if no == errno.EINTR:
                    continue

            try:
                if input_source in rfds:
                    data = os.read(input_source, 1024)
                    data = str(data, encoding='utf8')
                    k8s_stream.write_stdin(data)

                if k8s_stream.sock.sock in rfds:
                    # read from k8s_stream
                    if k8s_stream.peek_stdout():
                        data = k8s_stream.read_stdout()
                        data = bytes(data, encoding='utf8')
                        os.write(input_source, data)
                    # error occurs
                    if k8s_stream.peek_channel(3):
                        break
            except Exception as e:
                logger.error(e)

        resp.close()

    def pod_log(self, pod_name, namespace, tail_lines=None, container=None):
        if not tail_lines and not container:
            ret = self.k8s_coreapi.read_namespaced_pod_log(pod_name, namespace)
        elif tail_lines and container:
            ret = self.k8s_coreapi.read_namespaced_pod_log(pod_name, namespace,
                                                           tail_lines=tail_lines, container=container)
        elif tail_lines:
            ret = self.k8s_coreapi.read_namespaced_pod_log(pod_name, namespace, tail_lines=tail_lines)
        else:
            ret = self.k8s_coreapi.read_namespaced_pod_log(pod_name, namespace, container=container)

        return ret

    def allocatable_resources(self):
        cluster_resources = {"cpu": 0,
                             "memory": 0,
                             "gpu": 0}
        nodes = self.k8s_coreapi.list_node().items
        for node in nodes:
            name = node.metadata.name
            cluster_resources[name] = {}
            resources = node.status.allocatable
            if "nvidia.com/gpu" in resources.keys():
                gpu_number = int(resources["nvidia.com/gpu"])
                cluster_resources["gpu"] += gpu_number
            else:
                gpu_number = 0

            if "cpu" in resources.keys():
                cpu_allocatable = int(resources["cpu"].strip("m"))
                cluster_resources["cpu"] += cpu_allocatable
            else:
                cpu_allocatable = 0

            if "memory" in resources.keys():
                memory_allocatable = int(resources["memory"])
                cluster_resources["memory"] += memory_allocatable
            else:
                memory_allocatable = 0

            cluster_resources[name]["gpu"] = gpu_number
            cluster_resources[name]["cpu"] = cpu_allocatable
            cluster_resources[name]["memory"] = memory_allocatable
            labels = node.metadata.labels
            if "gpu" in labels.keys():
                cluster_resources[name]["gpuType"] = labels["gpu"]

        return cluster_resources

    def remain_resources(self):
        remain_resources = deepcopy(self.allocatable_resources)
        pods = self.k8s_coreapi.list_pod_for_all_namespaces().items
        for pod in pods:
            if not self.resources_released(pod):
                node_name = pod.spec.node_name
                containers = pod.spec.containers
                gpu_limits = 0
                cpu_requests = 0
                memory_request = 0
                for con in containers:
                    resources_limits = con.resources.limits
                    resources_requests = con.resources.requests
                    if resources_limits and "nvidia.com/gpu" in resources_limits.keys():
                        gpu_limits += int(resources_limits["nvidia.com/gpu"])

                    if resources_requests and "cpu" in resources_requests.keys():
                        requested_cpu = resources_requests["cpu"]
                        if "m" not in requested_cpu:
                            requested_cpu = int(float(requested_cpu) * 1000)
                        else:
                            requested_cpu = int(requested_cpu.strip("m"))
                        cpu_requests += requested_cpu

                    if resources_requests and "memory" in resources_requests.keys():
                        memory_request += self.transfer_memory_to_byte(resources_requests["memory"])
                remain_resources["gpu"] -= gpu_limits
                remain_resources["cpu"] -= cpu_requests
                remain_resources["memory"] -= memory_request
                remain_resources[node_name]["gpu"] -= gpu_limits
                remain_resources[node_name]["cpu"] -= cpu_requests
                remain_resources[node_name]["memory"] -= memory_request

        return remain_resources

    def transfer_memory_to_byte(self, memory):
        result = 0
        memory_list = [("Ki", 1024),
                       ("K",  1000),
                       ("Mi", 1024*1024),
                       ("M",  1000*1000),
                       ("Gi", 1024*1024*1024),
                       ("G",  1000*1000*1000),
                       ("Ti", 1024*1024*1024*1024),
                       ("T",  1000*1000*1000*1000),
                       ("Pi", 1024*1024*1024*1024*1024),
                       ("P",  1000*1000*1000*1000*1000),
                       ("Ei", 1024*1024*1024*1024*1024*1024),
                       ("E",  1000*1000*1000*1000*1000*1000)]
        small_request = True
        for item in memory_list:
            if item[0] in memory:
                small_request = False
                result = int(float(memory.strip(item[0])) * item[1])
                break
        if small_request:
            return int(memory)
        else:
            return result

    def resources_released(self, pod):
        resources_released = True
        if not pod.status.container_statuses:
            return resources_released
        for item in pod.status.container_statuses:
            if item.state.running:
                resources_released = False
        return resources_released

    def gpu_requests_meet(self, num_workers, num_gpu_per_worker, gpu_type=None):
        remain_resources = self.get_remain_resources()
        if remain_resources["gpu"] < (num_gpu_per_worker * num_workers):
            return False
        count = 0
        for k, v in remain_resources.items():
            if isinstance(v, dict):
                if gpu_type:
                    if "gpuType" in v.keys():
                        if gpu_type == v["gpuType"] and v["gpu"] >= num_gpu_per_worker:
                            count += 1
                else:
                    if "gpu" in v.keys() and v["gpu"] >= num_gpu_per_worker:
                        count += 1
        if count >= num_workers:
            return True
        else:
            return False

    def resources_metrics(self, pod_name, namespace, command=COMMAND, process="python"):
        """
        Get cpu, memory, gpu util, gpu memory usage of 'python' process in 'pod_name' of 'namespace'
        :param pod_name:
        :param namespace:
        :param command:
        :param process:
        :return:
        """
        cpu = None
        memory = None
        gpu_util_memory = []
        try:
            resp = stream(self.k8s_coreapi.connect_get_namespaced_pod_exec, pod_name, namespace,
                          command=command,
                          stderr=True, stdin=False,
                          stdout=True, tty=False)
            resp = resp.splitlines()
            for r in resp:
                if process in r:
                    temp = r.strip().split(" ")
                    cpu = temp[0] + "%"
                    memory = temp[1] + "%"
                if "MiB" in r:
                    temp = r.split(" ")
                    gpu_memory = temp[0] + "/" + temp[1]
                    gpu_util = temp[2]
                    device_usage = {"util": gpu_util, "memory": gpu_memory}
                    gpu_util_memory.append(device_usage)
        except:
            logger.error("can not exec into pod %s in namespace %s" % (pod_name, namespace))
        return cpu, memory, gpu_util_memory

    def get_namespaces(self):
        result = []
        namespaces = self.k8s_coreapi.list_namespace().items
        for it in namespaces:
            result.append(it.metadata.name)
        return result

    def delete_completed_jobs(self, namespace):
        jobs = self.k8s_batch.list_namespaced_job(namespace=namespace).items
        related_services = []
        for job in jobs:
            logger.debug("current job " + job.metadata.name)
            if job.status.active is None:
                completed = True
                conditions = job.status.conditions
                for con in conditions:
                    if con.type != "Complete":
                        completed = False
            else:
                completed = False
            if completed:
                logger.debug("deleting completed job " + job.metadata.name)
                resp = self.k8s_batch.delete_namespaced_job(name=job.metadata.name, namespace=namespace,
                                                       body=client.V1DeleteOptions(propagation_policy='Background'))
                logger.debug("the action of deleting " + job.metadata.name + " job is " + resp.status)
                if "app" in job.metadata.labels.keys():
                    related_services.append(job.metadata.labels["app"])
        return related_services

    def delete_related_services(self, namespace):
        related_services = self.delete_completed_jobs(namespace=namespace)
        services = self.k8s_coreapi.list_namespaced_service(namespace=namespace).items
        for service in services:
            selector = service.spec.selector
            if not selector:
                continue
            if "app" in selector.keys():
                if selector["app"] in related_services:
                    resp = self.k8s_coreapi.delete_namespaced_service(name=service.metadata.name, namespace=namespace,
                                                                 body=client.V1DeleteOptions(
                                                                     propagation_policy='Background'))
                    logger.debug("the action of deleting " + service.metadata.name + " service is " + resp.status)

    def job_eviction(self, namespace):
        if not namespace:
            namespaces = self.get_namespaces()
            for n in namespaces:
                logger.debug(n)
                self.delete_related_services(namespace=n)
        else:
            self.delete_related_services(namespace)


if __name__ == '__main__':
    pass