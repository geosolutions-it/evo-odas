from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import Pusher

# Dag is returned by a factory method
def pushers_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
  dag = DAG(
    '%s.%s' % (parent_dag_name, child_dag_name),
    schedule_interval=schedule_interval,
    start_date=start_date,
  )

  push1 = Pusher(a_msg='message number 1', task_id='task_push1', dag=dag)
  push2 = Pusher(a_msg='message number 2', task_id='task_push2', dag=dag)
  push3 = Pusher(a_msg='message number 3', task_id='task_push3', dag=dag)
  push4 = Pusher(a_msg='message number 4', task_id='task_push4', dag=dag)
  push5 = Pusher(a_msg='message number 5', task_id='task_push5', dag=dag)

  push1
  push2
  push3
  push4
  push5

  return dag
