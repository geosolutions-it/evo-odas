import logging
import abc
from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)

class Pusher(BaseOperator):

    @apply_defaults
    def __init__(self, a_msg, *args, **kwargs):
        log.info('--- POC_PLUGIN Pusher ---')
        self.msg = a_msg
        super(Pusher, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("message to push: '%s'", self.msg)
        task_instance = context['task_instance']
        task_instance.xcom_push(key="the_message", value=self.msg)

class Puller(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        log.info('--- POC_PLUGIN Puller ---')
        super(Puller, self).__init__(*args, **kwargs)

    @abc.abstractmethod
    def execute(self, context): pass

class PullerFromDAG(Puller):

    def execute(self, context):
        task_instance = context['task_instance']
        # pull from a task in the same DAG
        msg = task_instance.xcom_pull(task_ids='task_push1', key='the_message')

class PullerFromSubDAG(Puller):

    def execute(self, context):
        task_instance = context['task_instance']
        # pull from a task in a subdag DAG
        #msg = task_instance.xcom_pull(task_ids='task_push1', key='the_message', dag_id='xcom_canvas_dag.xcom_canvas_subdag')
        # pull from a set of tasks in a subdag
        for idx in [1,2,3,4,5]:
            msg = task_instance.xcom_pull(task_ids='task_push'+str(idx), key='the_message', dag_id='poc_canvas_dag.poc_canvas_subdag') #pull from a task in a subdag DAG
            log.info("the pulled message is: '%s'", msg)

class XComPlugin(AirflowPlugin):
    name = "poc_plugin"
    operators = [Pusher, PullerFromSubDAG, PullerFromDAG]
