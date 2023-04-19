sql_vehicles = """SELECT vehicles.id as vehicle_id, 
CONCAT(vehicles.brand_name , '_' , vehicles.model_name) as name_id, 
vehicles.brand_name, 
vehicles.model_name, 
vehicles.complectation_name, 
vehicles.engine_capacity, 
vehicles.transmission_type, 
vehicles.year_of_production, 
vehicles.VIN, 
Max(contracts.contract_status) as contract_status, 
vehicles.state_number, 
vehicle_date.osago_date as registration_of_the_vehicle, 
vehicles.created_at, 
max(to_char(service_data.last_maintenance_date, 'YYYY-MM-DD')) as last_maintenance_date , 
max(service_data.last_registered_mileage) as last_registered_mileage, 
max(to_char(service_data.last_registered_mileage_date, 'YYYY-MM-DD')) as last_registered_mileage_date,
case 
when avo.vin is not null THEN 'avo'
Else 'sbt'
END as telematics
FROM pbi.erp_registry_leaseobjects_vehicle AS vehicles
JOIN pbi.erp_registry_leaseobjects_vehiclestatus as status ON status.id = vehicles.status_id
LEFT JOIN pbi.erp_registry_leaseobjects_vehicleservice AS service_data ON vehicles.id = service_data.vehicle_id
LEFT JOIN pbi.sbt_all_cars as sbt ON vehicles.vin = sbt.vin
LEFT JOIN pbi.avo_all_cars as avo ON vehicles.vin = avo.vin
LEFT JOIN pbi.erp_leasecontract_contracts as contracts ON vehicles.vin = contracts.vin
LEFT JOIN (SELECT vehicle_id, min(start_date) as osago_date
FROM pbi.erp_registry_leaseobjects_vehicleinsurance 
WHERE type = 'osago' and is_removed is false
GROUP BY vehicle_id) vehicle_date ON vehicle_date.vehicle_id = vehicles.id
WHERE vehicles.VIN is not Null and vehicles.is_removed = False AND vehicle_date.osago_date is not Null AND status.name NOT IN ('sold', 'draft')
Group by vehicles.id , vehicles.vin, name_id, vehicles.brand_name , vehicles.model_name, vehicles.complectation_name, vehicles.engine_capacity, 
vehicles.transmission_type, vehicles.year_of_production, vehicles.state_number, vehicle_date.osago_date,
vehicles.created_at, avo.vin"""

sql_events = """SELECT events.state_number, vehicles.brand_name, vehicles.model_name, vehicles.complectation_name, events.vin, events.registry_vehicle_id as vehicle_id, 
events.auto_created,  to_char(events.event_date, 'YYYY-MM-DD') as event_date, events.authorization_date, 
events.validation_date, operations.job_code, 
operations.cost, operations.rebilling_cost, 
ev_sts.name as event_status, ev_type.name as event_type, op_type.name as operation_type
FROM pbi.erp_events_events as events
INNER JOIN pbi.erp_events_operations as operations ON events.id = operations.event_id
INNER JOIN pbi.erp_events_event_statuses as ev_sts ON events.event_status_id = ev_sts.id
INNER JOIN pbi.erp_mdm_services_servicecategory as ev_type ON events.event_type_id = ev_type.id
INNER JOIN pbi.erp_mdm_services_service as op_type ON operations.job_code = op_type.job_code
LEFT JOIN pbi.erp_registry_leaseobjects_vehicle as vehicles ON events.registry_vehicle_id = vehicles.id
WHERE events.is_deleted = False"""

sql_last_pm = """SELECT vehicles.id as vehicle_id, vehicles.brand_name, vehicles.model_name, vehicles.complectation_name, vehicles.VIN, 
vehicles.state_number, MAX(operations.job_code ) as job_code, 
MAX(operation_names.name) as op_name, to_char(MAX(events.event_date), 'YYYY-MM-DD') as event_date
FROM pbi.erp_registry_leaseobjects_vehicle AS vehicles
LEFT JOIN pbi.erp_events_events AS events ON vehicles.id = events.registry_vehicle_id
LEFT JOIN pbi.erp_events_operations AS operations ON events.id = operations.event_id
LEFT JOIN pbi.erp_mdm_services_service AS operation_names ON operations.job_code = operation_names.job_code
WHERE vehicles.is_removed is False and vehicles.VIN is not Null
and operations.job_code IN ('100','101','102','103','104','105','106','107','108','109','110','111','112','113','114',
							'115','116','117','118','119','120','121','122','123','124','125','126','127','128','129',
							'130','131','132','133','134','135','136','137','138','139','156')
and events.is_deleted = False
GROUP BY vehicles.id, vehicles.brand_name, vehicles.model_name, vehicles.complectation_name, vehicles.VIN, vehicles.state_number"""


sql_check_pm_list = """WITH fleet as (
	SELECT 
	CONCAT(brand_name, '_', model_name) as id
	FROM pbi.erp_registry_leaseobjects_vehicle
	GROUP BY CONCAT(brand_name, '_', model_name))

SELECT 
fleet.id as sap_fleet_vehicle_name
FROM fleet
LEFT JOIN pbi.opertions_vehicles_maintenance_period_mileage as pm ON fleet.id = pm.name_id
WHERE pm.name_id is null
and fleet.id <> '_'
and fleet.id NOT LIKE 'BELARUS%'
and fleet.id NOT LIKE 'Газон%'"""

sql_pm_period = """SELECT * FROM pbi.opertions_vehicles_maintenance_period_mileage"""


sql_zero_pm = """SELECT * FROM pbi.opertions_vehicles_maintenance_zero_pm_period"""