from mesos.interface import mesos_pb2, SchedulerDriver, Scheduler
import uuid


class PythonScheduler(Scheduler):
    def __init__(self):
        self.tasks = []

    def statusUpdate(self, driver, status):
        print "received status update:{}".format(status)

    def resourceOffers(self, driver, offers):
        for offer in offers:
            print "offer:{}".format(offer)
            if len(self.tasks) > 0:
                tid = "1"
                task = mesos_pb2.TaskInfo()
                task.task_id.value = tid
                task.slave_id.value = offer.slave_id.value
                task.name = "hello"
                #task.executor.MergeFrom(self.executor)
                command = self.tasks.pop()
                print "command: ", command
                task.command.value = command

                cpus = task.resources.add()
                cpus.name = "cpus"
                cpus.type = mesos_pb2.Value.SCALAR
                cpus.scalar.value = 1.0
                cpus.role = "*"

                # request the driver to launch the task
                driver.launchTasks(offer.id, [task])


    def submitTasks(self, task):
        self.tasks.append(task)

    def registered(self, driver, frameworkId, masterInfo):
        pass

    def reregistered(self, driver, masterInfo):
        pass

    def error(self, driver, message):
        pass

    def executorLost(self, driver, executorId, slaveId, status):
        pass

    def slaveLost(self, driver, slaveId):
        pass

    def disconnected(self, driver):
        pass

    def frameworkMessage(self, driver, executorId, slaveId, data):
        pass