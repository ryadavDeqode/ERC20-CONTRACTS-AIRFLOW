from airflow import DAG
from datetime import date, timedelta
from airflow.hooks.clickhouse_hook import ClickHouseHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from attributedict.collections import AttributeDict
from airflow.utils.dates import days_ago
import os
from web3 import Web3

with open('/usr/local/airflow/scripts/key.txt', 'r') as file:
  infura_key = file.read()
  file.close()

web3 = Web3(Web3.HTTPProvider(infura_key))


ch_hook = ClickHouseHook()

balanceOfByte = "70a08231"
totalSupplyByte = "18160ddd"
transferByte = "a9059cbb"
transferFromByte = "23b872dd"
approveByte = "095ea7b3"
allowanceByte = "dd62ed3e"

def getData(**kwargs):
    # ch_hook.run('CREATE DATABASE IF NOT EXISTS ERC20_DATA')
    # ch_hook.run('USE ERC20_DATA')
    # CREATE TABLE erc20_contracts(deployedAtBlock UInt64, contractAddress String, creatorAddress String, transactionHash String) ENGINE = Log
    for atBlock in range(kwargs['start'],kwargs['end']):
      txnsA = web3.eth.get_block(atBlock)
      txns = AttributeDict(txnsA)
      for txnHash in txns['transactions']:
        contractAdd = AttributeDict(web3.eth.get_transaction_receipt(txnHash)).contractAddress
        if contractAdd != None:
          ercCode = web3.eth.get_code(contractAdd).hex()
          if balanceOfByte in ercCode and totalSupplyByte in ercCode and transferByte in ercCode and transferFromByte in ercCode and approveByte in ercCode and allowanceByte in ercCode:
            creator = AttributeDict(web3.eth.get_transaction_receipt(txnHash))['from']
            ch_hook.run('INSERT INTO ERC20_DATA.erc20_contracts (deployedAtBlock,contractAddress,creatorAddress,transactionHash) VALUES',[(atBlock,contractAdd,creator,txnHash.hex())])

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': [''],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
  dag_id='batch_1',
  start_date=days_ago(1),
  default_args=default_args,
)

start = BashOperator(
    task_id="start",
    bash_command='echo "starting"'
)

getDataFirstBatch = PythonOperator(
    task_id = 'batch_1',
    python_callable=getData,
    op_kwargs= {'start' : 1405561, 'end' : 1406562},
    dag=dag
  )
getDataSecondBatch = PythonOperator(
    task_id = 'batch_2',
    python_callable=getData,
    op_kwargs= {'start' : 1406562, 'end' : 1407563},
    dag=dag
  )
getDataThirdBatch = PythonOperator(
    task_id = 'batch_3',
    python_callable=getData,
    op_kwargs= {'start' : 1407563, 'end' : 1408564},
    dag=dag
  )
getDataFourthBatch = PythonOperator(
    task_id = 'batch_4',
    python_callable=getData,
    op_kwargs= {'start' : 1408564, 'end' : 1409565},
    dag=dag
  )

end = BashOperator(
    task_id="end",
    bash_command='echo "finished"'
)

start >> [getDataFirstBatch,getDataSecondBatch,getDataThirdBatch,getDataFourthBatch] >> end