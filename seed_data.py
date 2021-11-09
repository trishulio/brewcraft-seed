# Before running DAG first create connection in airflow with the following values:
# Conn id = brewcraft
# Conn type = http
# Host = apollo.brewcraft.io (or other base url for brewcraft)
# Schema = https (or http)
# 
# Trigger DAG with config values:
# {"jwt": "<YOUR_JWT_HERE>"}
#
import airflow
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import json
import os

CURRENT_DIRECTORY = os.path.abspath(os.path.dirname(__file__))

def parseRequestsFromFile(filename):
	input_file = open(f"{CURRENT_DIRECTORY}/payloads/" + filename)
	return json.load(input_file)

default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2017, 10, 9),
    'email': 'test@test.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG('brewcraft_seed_data',
    schedule_interval=None,
    default_args=default_args)
		
start_task = DummyOperator(task_id='start',retries=3, dag=dag)

start_facility_requests_task = DummyOperator(task_id='start_facility_requests',retries=3, dag=dag)
start_equipment_requests_task = DummyOperator(task_id='start_equipment_requests',retries=3, dag=dag)
start_storage_requests_task = DummyOperator(task_id='start_storage_requests',retries=3, dag=dag)
start_supplier_requests_task = DummyOperator(task_id='start_supplier_requests',retries=3, dag=dag)
start_supplier_contact_requests_task = DummyOperator(task_id='start_supplier_contact_requests',retries=3, dag=dag)
start_material_requests_task = DummyOperator(task_id='start_material_requests',retries=3, dag=dag)
start_product_requests_task = DummyOperator(task_id='start_product_requests',retries=3, dag=dag)
start_invoice_requests_task = DummyOperator(task_id='start_invoice_requests',retries=3, dag=dag)
start_shipment_requests_task = DummyOperator(task_id='start_shipment_requests',retries=3, dag=dag)

facility_requests_complete_task = DummyOperator(task_id='facility_requests_complete',retries=3, dag=dag)
equipment_requests_complete_task = DummyOperator(task_id='equipment_requests_complete',retries=3, dag=dag)
storage_requests_complete_task = DummyOperator(task_id='storage_requests_complete',retries=3, dag=dag)
supplier_requests_complete_task = DummyOperator(task_id='supplier_requests_complete',retries=3, dag=dag)
supplier_contact_requests_complete_task = DummyOperator(task_id='supplier_contact_requests_complete',retries=3, dag=dag)
material_requests_complete_task = DummyOperator(task_id='material_requests_complete',retries=3, dag=dag)
product_requests_complete_task = DummyOperator(task_id='product_requests_complete',retries=3, dag=dag)
invoice_requests_complete_task = DummyOperator(task_id='invoice_requests_complete',retries=3, dag=dag)
shipment_requests_complete_task = DummyOperator(task_id='shipment_requests_complete',retries=3, dag=dag)

	
add_facility_requests = parseRequestsFromFile("facilities.json")
facilityTasks = []
for idx,request in enumerate(add_facility_requests):
	facilityTasks.append(SimpleHttpOperator(
		task_id='add_facility_request_' + str(idx),
		endpoint='/api/v1/facilities',
		method='POST',
		response_filter=lambda response: response.json(),
		data=json.dumps(request),
		headers={"Content-Type": "application/json", "Authorization": "Bearer " + '{{ dag_run.conf["jwt"] }}'},
		http_conn_id='brewcraft',
		dag=dag))
	start_facility_requests_task >> facilityTasks[idx]
	facilityTasks[idx] >> facility_requests_complete_task


add_equipment_requests = parseRequestsFromFile("equipment.json")
equipmentTasks = []
for idx,request in enumerate(add_equipment_requests):
	equipmentTasks.append(SimpleHttpOperator(
		task_id='add_equipment_request_' + str(idx),
		endpoint='/api/v1/facilities/' + f'{{{{task_instance.xcom_pull(task_ids="{facilityTasks[0].task_id}")["id"]}}}}' +'/equipment',
		method='POST',
		response_filter=lambda response: response.json(),
		data=json.dumps(request),
		headers={"Content-Type": "application/json", "Authorization": "Bearer " + '{{ dag_run.conf["jwt"] }}'},
		http_conn_id='brewcraft',
		dag=dag))
	start_equipment_requests_task >> equipmentTasks[idx]
	equipmentTasks[idx] >> equipment_requests_complete_task
	

add_storage_requests = parseRequestsFromFile("storages.json")
storageTasks = []
for idx,request in enumerate(add_storage_requests):
	storageTasks.append(SimpleHttpOperator(
		task_id='add_storage_request_' + str(idx),
		endpoint='/api/v1/facilities/' + f'{{{{task_instance.xcom_pull(task_ids="{facilityTasks[0].task_id}")["id"]}}}}' + '/storages',
		method='POST',
		response_filter=lambda response: response.json(),
		data=json.dumps(request),
		headers={"Content-Type": "application/json", "Authorization": "Bearer " + '{{ dag_run.conf["jwt"] }}'},
		http_conn_id='brewcraft',
		dag=dag))
	start_storage_requests_task >> storageTasks[idx]
	storageTasks[idx] >> storage_requests_complete_task
	

add_supplier_requests = parseRequestsFromFile("suppliers.json")
supplierTasks = []
for idx,request in enumerate(add_supplier_requests):
	supplierTasks.append(SimpleHttpOperator(
		task_id='add_supplier_request_' + str(idx),
		endpoint='/api/v1/suppliers',
		method='POST',
		response_filter=lambda response: response.json(),
		data=json.dumps(request),
		headers={"Content-Type": "application/json", "Authorization": "Bearer " + '{{ dag_run.conf["jwt"] }}'},
		http_conn_id='brewcraft',
		dag=dag))
	start_supplier_requests_task >> supplierTasks[idx]
	supplierTasks[idx] >> supplier_requests_complete_task
	
	
add_supplier_contact_requests = parseRequestsFromFile("suppliercontacts.json")
supplierContactTasks = []
for idx,request in enumerate(add_supplier_contact_requests):
	supplierContactTasks.append(SimpleHttpOperator(
		task_id='add_supplier_contact_request_' + str(idx),
		endpoint='/api/v1/suppliers/' + f'{{{{task_instance.xcom_pull(task_ids="{supplierTasks[idx].task_id}")["id"]}}}}' + '/contacts',
		method='POST',
		response_filter=lambda response: response.json(),
		data=json.dumps(request),
		headers={"Content-Type": "application/json", "Authorization": "Bearer " + '{{ dag_run.conf["jwt"] }}'},
		http_conn_id='brewcraft',
		dag=dag))
	start_supplier_contact_requests_task >> supplierContactTasks[idx]
	supplierContactTasks[idx] >> supplier_contact_requests_complete_task
	
	
add_material_requests = parseRequestsFromFile("materials.json")
materialTasks = []
for idx,request in enumerate(add_material_requests):
	materialTasks.append(SimpleHttpOperator(
		task_id='add_material_request_' + str(idx),
		endpoint='/api/v1/materials',
		method='POST',
		response_filter=lambda response: response.json(),
		data=json.dumps(request),
		headers={"Content-Type": "application/json", "Authorization": "Bearer " + '{{ dag_run.conf["jwt"] }}'},
		http_conn_id='brewcraft',
		dag=dag))
	start_material_requests_task >> materialTasks[idx]
	materialTasks[idx] >> material_requests_complete_task
	
	
add_product_requests = parseRequestsFromFile("products.json")
productTasks = []
for idx,request in enumerate(add_product_requests):
	productTasks.append(SimpleHttpOperator(
		task_id='add_product_request_' + str(idx),
		endpoint='/api/v1/products',
		method='POST',
		response_filter=lambda response: response.json(),
		data=json.dumps(request),
		headers={"Content-Type": "application/json", "Authorization": "Bearer " + '{{ dag_run.conf["jwt"] }}'},
		http_conn_id='brewcraft',
		dag=dag))
	start_product_requests_task >> productTasks[idx]
	productTasks[idx] >> product_requests_complete_task
	
	
add_invoice_requests = parseRequestsFromFile("invoices.json")
invoiceTasks = []
for idx,request in enumerate(add_invoice_requests):
	#Inject materialIds into invoice payloads
	request["items"][0]["materialId"] = request["items"][0]["materialId"].format(materialTasks[idx].task_id)
	request["items"][1]["materialId"] = request["items"][1]["materialId"].format(materialTasks[idx + 1].task_id)
	
	invoiceTasks.append(SimpleHttpOperator(
		task_id='add_invoice_request_' + str(idx),
		endpoint='/api/v1/purchases/invoices',
		method='POST',
		response_filter=lambda response: response.json(),
		data=json.dumps([request]),
		headers={"Content-Type": "application/json", "Authorization": "Bearer " + '{{ dag_run.conf["jwt"] }}'},
		http_conn_id='brewcraft',
		dag=dag))
	start_invoice_requests_task >> invoiceTasks[idx]
	invoiceTasks[idx] >> invoice_requests_complete_task
	
	
add_shipment_requests = parseRequestsFromFile("shipments.json")
shipmentTasks = []
for idx,request in enumerate(add_shipment_requests):
	#Inject materialIds and invoiceItemIds into shipment payloads
	request["lots"][0]["materialId"] = request["lots"][0]["materialId"].format(invoiceTasks[idx].task_id)
	request["lots"][1]["materialId"] = request["lots"][1]["materialId"].format(invoiceTasks[idx].task_id)
	request["lots"][0]["invoiceItemId"] = request["lots"][0]["invoiceItemId"].format(invoiceTasks[idx].task_id)
	request["lots"][1]["invoiceItemId"] = request["lots"][1]["invoiceItemId"].format(invoiceTasks[idx].task_id)
	
	shipmentTasks.append(SimpleHttpOperator(
		task_id='add_shipment_request_' + str(idx),
		endpoint='/api/v1/purchases/shipments',
		method='POST',
		response_filter=lambda response: response.json(),
		data=json.dumps([request]),
		headers={"Content-Type": "application/json", "Authorization": "Bearer " + '{{ dag_run.conf["jwt"] }}'},
		http_conn_id='brewcraft',
		dag=dag))
	start_shipment_requests_task >> shipmentTasks[idx]
	shipmentTasks[idx] >> shipment_requests_complete_task


start_task >> start_facility_requests_task
start_task >> start_supplier_requests_task
start_task >> start_material_requests_task
start_task >> start_product_requests_task

facility_requests_complete_task >> start_equipment_requests_task
facility_requests_complete_task >> start_storage_requests_task

supplier_requests_complete_task >> start_supplier_contact_requests_task

material_requests_complete_task >> start_invoice_requests_task
material_requests_complete_task >> start_shipment_requests_task

invoice_requests_complete_task >> start_shipment_requests_task
