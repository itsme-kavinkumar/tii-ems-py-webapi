from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from src.models.image import save_image

def meter_group_Lists(cnx, meter_group_id):
    try:
        where = ""
        if meter_group_id !='':
            where = f" AND mmg.meter_group_id = {meter_group_id}"   
        # where += f" and mm.machine_id in ({','.join(str(x) for x in machine_id)})  
        query = text(f"""
            SELECT                
                mm.machine_code AS machine_code,
                mm.machine_name AS machine_name,
                (CASE 
                WHEN group_type='Zone' THEN (SELECT department_name FROM ems_v1.dbo.master_department WHERE department_id=type_id)
                WHEN group_type='Area' THEN (SELECT shed_name FROM ems_v1.dbo.master_shed WHERE shed_id=type_id)
                WHEN group_type='Location' THEN (SELECT machinetype_name FROM ems_v1.dbo.master_machinetype WHERE machinetype_id=type_id)
                WHEN group_type='Function' THEN (SELECT function_name FROM ems_v1.dbo.master_function WHERE function_id=type_id)
                WHEN group_type='Function_1' THEN (SELECT function_name FROM ems_v1.dbo.master_function WHERE function_id=type_id)
                WHEN group_type='Function_2' THEN (SELECT function_name FROM ems_v1.dbo.master_function WHERE function_id=type_id)
                END) AS type_name,
                mmg.*,
                '' AS machine_dtl,
                ISNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	            ISNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user

            FROM 
                [ems_v1].[dbo].[master_meter_group] mmg
                left join [ems_v1].[dbo].[master_employee] cu on cu.employee_id=mmg.created_by
	            left join [ems_v1].[dbo].[master_employee] mu on mu.employee_id=mmg.modified_by                
                INNER JOIN [ems_v1].[dbo].[master_machine] mm ON (mmg.machine_id LIKE CONCAT('%,', CAST(mm.machine_id AS VARCHAR)) OR 
                 
                mmg.machine_id = CAST(mm.machine_id AS VARCHAR))
            WHERE 
                mmg.status != 'delete'{where}
        """)
        print(query)
        data = cnx.execute(query).mappings().all()
        result = []
        for row in data:
            machine_id_list = row["machine_id"].split(",")   # Split comma-separated machine IDs into a list
            machine_dtl = ""
            for machine_id in machine_id_list:                             
                sub_query = text(f"SELECT * FROM [ems_v1].[dbo].[master_machine] WHERE machine_id = {machine_id}")
                sub_data = cnx.execute(sub_query).mappings().all()
                for sub_row in sub_data:
                    if machine_dtl != "":
                        machine_dtl += '\n' 
                    machine_dtl += f'''{sub_row['machine_code']} - {sub_row['machine_name']} '''  
                    print(machine_dtl)          
            new_row = dict(row)
            new_row["machine_dtl"] = machine_dtl
            result.append(new_row)
            
        return result
    except Exception as e:
        return get_exception_response(e)
    
def save_meter_group(cnx,machine_id,group_type,type_id,user_login_id):

    try: 
        data = ''
        def id(machine_id):
            if machine_id !='':
                value = machine_id.split(",")
                if len(value) > 1:
                    values = tuple(value)
                    machine_id = ",".join(values)
                else:
                    machine_id = value[0]
            return machine_id
        
        type_id = id(type_id)
        machine_id = id(machine_id)

        query = text(f"""
                INSERT INTO [ems_v1].[dbo].[master_meter_group] (
                group_type, type_id, machine_id, created_on, created_by
                )
                VALUES (
                    '{group_type}', '{type_id}', '{machine_id}',  GETDATE(), '{user_login_id}'
                )
            """) 
        cnx.execute(query)
        cnx.commit()

        return data
    except Exception as e:
        return get_exception_response(e)
    
def update_meter_group(cnx,meter_group_id,machine_id,group_type,type_id,user_login_id):
    try:
        query =text(f"""
            UPDATE [ems_v1].[dbo].[master_meter_group]
            SET group_type = '{group_type}', type_id = '{type_id}',
            machine_id = '{machine_id}',  modified_on = GETDATE(),
            modified_by = '{user_login_id}'
            WHERE meter_group_id = {meter_group_id} 
        """)
        
        cnx.execute(query)
        cnx.commit()
    except Exception as e:
        return get_exception_response(e)
    
def update_meter_groupStatus(cnx, meter_group_id, status):
    if status != '':
        query=f''' Update ems_v1.dbo.master_meter_group Set status = '{status}' Where meter_group_id='{meter_group_id}' '''
    else: 
        query=f''' Update ems_v1.dbo.master_meter_group Set status = 'delete' Where meter_group_id='{meter_group_id}' '''
      
    cnx.execute(text(query))
    cnx.commit()
    
    

