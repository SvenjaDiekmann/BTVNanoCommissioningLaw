# coding: utf-8

from __future__ import absolute_import

__all__ = ["CampaignTask", "HTCondorWorkflow"]


import os
import importlib

import luigi
import law

law.contrib.load("numpy", "tasks", "root", "wlcg", "htcondor", "hdf5", "coffea")


class BaseTask(law.Task):
    version = luigi.Parameter(description="task version")

    output_collection_cls = law.SiblingFileCollection

    def local_path(self, *path):
        parts = [str(p) for p in self.store_parts() + path]
        return os.path.join(os.environ["BTV_STORE"], *parts)

    def wlcg_path(self, *path):
        parts = [str(p) for p in self.store_parts() + path]
        return os.path.join(*parts)

    def local_target(self, *args):
        cls = law.LocalFileTarget if args else law.LocalDirectoryTarget
        return cls(self.local_path(*args))

    def local_directory_target(self, *args):
        return law.LocalDirectoryTarget(self.local_path(*args))

    def wlcg_target(self, *args, **kwargs):
        cls = law.wlcg.WLCGFileTarget if args else law.wlcg.WLCGDirectoryTarget
        return cls(self.wlcg_path(*args), **kwargs)

    def store_parts(self):
        parts = (self.__class__.__name__,)
        if self.version is not None:
            parts += (self.version,)
        return parts


class CampaignTask(BaseTask):
    campaign = luigi.Parameter(default="UL16")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.campaign_inst = importlib.import_module(
            "btv.config.{}".format(self.campaign)
        ).campaign
        self.config_inst = importlib.import_module(
            "btv.config.{}".format(self.campaign)
        ).cfg

    def store_parts(self):
        parts = ("btv", self.campaign, self.__class__.__name__)
        if self.version is not None:
            parts += (self.version,)
        return parts


class HTCondorWorkflow(law.htcondor.HTCondorWorkflow):
    def htcondor_post_submit_delay(self):
        return self.poll_interval * 60

    def htcondor_output_directory(self):
        # the directory where submission meta data should be stored
        return law.LocalDirectoryTarget(self.local_path())

    def htcondor_job_config(self, config, job_num, branches):
        # copy the entire environment
        config.custom_content.append(("getenv", "true"))
        config.custom_content.append(("request_cpus", "1"))
        config.custom_content.append(("RequestMemory", "2000"))
        config.custom_content.append(("Request_GPUs", "0"))
        config.custom_content.append(("Request_GpuMemory", "0"))
        return config

    def htcondor_use_local_scheduler(self):
        return True
