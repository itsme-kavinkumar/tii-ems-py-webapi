from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from src.models.image import parse_date
from datetime import date

def orderwise(cnx,table_name,obj):
    try:
        data = ''
        if obj !='':
            obj_data = json.loads(obj)
            for row in obj_data:
                id = row["id"]
                sno = row["sno"]
                if table_name == 'zone':
                    sql = text(f" update ems_v1.dbo.master_department set department_order = {sno} where department_id = {id} ")
                    
                if table_name == 'area':
                    sql = text(f" update ems_v1.dbo.master_shed set shed_order = {sno} where shed_id = {id} ")
                
                if table_name == 'location':
                    sql = text(f" update ems_v1.dbo.master_machinetype set machinetype_order = {sno} where machinetype_id = {id} ")
                    
                if table_name == 'function_1':
                    sql = text(f" update ems_v1.dbo.master_function set function_order = {sno} where function_id = {id}")
                    
                if table_name == 'function_2':
                    sql = text(f" update ems_v1.dbo.master_function set function_order = {sno} where function_id = {id}")
                    
                if table_name == 'meter':
                    sql = text(f" update ems_v1.dbo.master_machine set machine_order = {sno} where machine_id = {id} ")

                cnx.execute(sql)
                cnx.commit()
       
        return data
    except Exception as e:
        return get_exception_response(e)