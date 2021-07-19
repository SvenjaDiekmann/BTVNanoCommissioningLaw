import numpy as np
import law
import luigi
import subprocess
import os
import importlib
import coffea.processor as processor
from coffea.util import save
from collections import defaultdict
from dask.distributed import Client
from distributed.security import Security
from dask_jobqueue import htcondor

from btv.tasks.base import CampaignTask

import shutil


class HTCondorJob(htcondor.HTCondorJob):
    sg_target = "openports"
    executable = shutil.which("sg")

    def job_script(self):
        """Construct a job submission script"""
        quoted_arguments = htcondor.quote_arguments(
            [self.sg_target, "-c", self._command_template]
        )
        quoted_environment = htcondor.quote_environment(self.env_dict)
        job_header_lines = "\n".join(
            "%s = %s" % (k, v) for k, v in self.job_header_dict.items()
        )

        return self._script_template % {
            "shebang": self.shebang,
            "job_header": job_header_lines,
            "quoted_environment": quoted_environment,
            "quoted_arguments": quoted_arguments,
            "executable": self.executable,
        }


class FileFetcher(CampaignTask):
    def output(self):
        return self.local_target("samples.json")

    def run(self):
        dataset_lfns = defaultdict(list)
        dasgoclient = os.environ["DASGOCLIENT"]

        for dataset in self.config_inst.datasets:
            instance = dataset.get_aux("instance", "prod/global")
            for key in dataset.keys:
                cmd = f"{dasgoclient} -query='instance={instance} file dataset={key}'"
                code, out, _ = law.util.interruptable_popen(
                    cmd, stdout=subprocess.PIPE, shell=True, executable="/bin/bash"
                )
                if code != 0:
                    raise Exception("DASgo query failed")

                lfns = out.strip().split("\n")
                dataset_lfns[dataset.name].extend(
                    f"root://cms-xrd-global.cern.ch/{lfn}" for lfn in lfns
                )

        self.output().dump(dataset_lfns, indent=4)


class CoffeaProcessor(CampaignTask):
    processor = luigi.Parameter(default="ttbar_validation")

    def requires(self):
        return FileFetcher.req(self)

    def output(self):
        return self.local_target("data.coffea")

    def run(self):
        def n2ip(n):
            return "134.61.19.%d" % n

        fileset = self.input().load()

        # for debugging (5 file)
        fileset = {k: v[:5] for k, v in fileset.items()}
        Processor = importlib.import_module(f"btv.processors.{self.processor}")
        processor_inst = Processor.NanoProcessor()

        cluster = htcondor.HTCondorCluster(
            job_cls=HTCondorJob,
            cores=2,
            processes=2,
            memory="2000MiB",
            disk="2GB",
            job_extra=dict(Request_CPUs=1, Request_GPUs=0, Getenv=True),
            security=Security(),
            protocol="tls://",
            scheduler_options=dict(
                dashboard_address=os.environ["DHA_DASHBOARD_ADDRESS"]
            ),
            local_directory="/tmp",
        )
        cluster.scale(jobs=5)

        client = Client(cluster, security=Security())

        output, metrics = processor.run_uproot_job(
            fileset,
            treename="Events",
            processor_instance=processor_inst,
            pre_executor=processor.futures_executor,
            pre_args=dict(workers=32),
            executor=processor.dask_executor,
            executor_args=dict(
                schema=processor.NanoAODSchema,
                mmap=True,
                savemetrics=1,
                client=client,
                workers=set(map(n2ip, np.r_[41:50, 61:64])),
                merge_workers={n2ip(51)},
            ),
            chunksize=100000,
        )
        self.output().parent.touch()
        save(output, self.output().path)
