from airflow import DAG
from datetime import datetime
from airflow.operators import PullerFromSubDAG, Pusher
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import BranchPythonOperator
from subdag_factory import pushers_sub_dag
import random

interval = '* * * * *'
date = datetime(2017, 5, 4)

main_dag = DAG('poc_canvas_dag', description='just a DAG to experiment with XCom',
          schedule_interval=interval,
          start_date=date, catchup=False)

options = ['task_push1', 'poc_canvas_dag']

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=lambda: random.choice(options),
    dag=main_dag)

single_pusher = Pusher(a_msg='message number 1', task_id='task_push1', dag=main_dag)

sub_dag = SubDagOperator(
    subdag = pushers_sub_dag('poc_canvas_dag', 'poc_canvas_subdag', date, interval),
    task_id='poc_canvas_subdag',
    dag=main_dag,
)

#pull = PullerFromDAG(task_id='puller_task', dag=dag)
pull = PullerFromSubDAG(task_id='puller_task', dag=main_dag)

branching >> single_pusher >> pull
branching >> sub_dag >> pull
