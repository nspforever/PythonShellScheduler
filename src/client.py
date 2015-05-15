from mesos.interface import SchedulerDriver, Scheduler
from mesos.native import MesosSchedulerDriver
from python_scheduler import PythonScheduler
from json import dumps, loads
from threading import Thread
from uuid import uuid1
import logging
import mesos
from mesos.interface.mesos_pb2 import FrameworkInfo
import os
import subprocess
import sys
import time

if __name__ == "__main__":
    print sys.argv[1:]
    framework = FrameworkInfo()
    framework.name = "DistributedShell"
    framework.user = "" # Let mesos fill current user
    framework.role = "*"
    framework.checkpoint = False
    framework.failover_timeout = 0.0

    #create instance of schedule and connect to mesos
    scheduler = PythonScheduler()
    #submit shell commands
    task = " ".join(sys.argv[1:])
    print "task: %s", task
    scheduler.submitTasks(task)
    mesosURL = "1.1.1.1:5050" # IP / hostname of mesos master
    driver = MesosSchedulerDriver(scheduler, framework, mesosURL)
    #run the driver
    driver.run()