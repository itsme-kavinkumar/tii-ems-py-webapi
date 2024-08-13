from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from log_file import createFolder
import json

def energy_calculationlist(cnx):
    try:
        query = text(f'''select * from [ems_v1].[dbo].[master_energy_calculations]''')
        data = cnx.execute(query).mappings().all()
        
        return data
    except Exception as e:
        return get_exception_response(e)


def save_energycalculation(cnx, obj):
    try: 
        data = ''
        del_query=text(f'''DELETE FROM [ems_v1].[dbo].[master_energy_calculations]''')
        cnx.execute(del_query)
        cnx.commit()
        obj_data = json.loads(obj)
        if obj !="":
            for row in obj_data:
                s_no = row["s_no"]
                group_name = row["group_name"]
                function_name = row["function_name"]
                formula1 = row["formula1"]
                formula2 = row["formula2"]
                parameter = row["parameter"]
                roundoff_value = row["roundoff_value"]
                query = text(f'''INSERT INTO [ems_v1].[dbo].[master_energy_calculations] 
                                (s_no,group_name,function_name,formula1,formula2,parameter,roundoff_value)
                                values({s_no},'{group_name}','{function_name}','{formula1}','{formula2}','{parameter}','{roundoff_value}')''')

                cnx.execute(query)
                cnx.commit()

        return data
    except Exception as e:
        return get_exception_response(e)
    
def energy_calculationlist2(cnx):
    try:
        query = text(f'''select * from [ems_v1].[dbo].[master_energy_calculations2]''')
        data = cnx.execute(query).mappings().all()
        
        return data
    except Exception as e:
        return get_exception_response(e)


def save_energycalculation2(cnx, obj):
    try: 
        data = ''
        del_query=text(f'''DELETE FROM [ems_v1].[dbo].[master_energy_calculations]''')
        cnx.execute(del_query)
        cnx.commit()
        obj_data = json.loads(obj)
        if obj !="":
            for row in obj_data:
                s_no = row["s_no"]
                group_name = row["group_name"]
                function_name = row["function_name"]
                formula1 = row["formula1"]
                formula2 = row["formula2"]
                parameter = row["parameter"]
                roundoff_value = row["roundoff_value"]
                query = text(f'''INSERT INTO [ems_v1].[dbo].[master_energy_calculations2] 
                                (s_no,group_name,function_name,formula1,formula2,parameter,roundoff_value)
                                values({s_no},'{group_name}','{function_name}','{formula1}','{formula2}','{parameter}','{roundoff_value}')''')

                cnx.execute(query)
                cnx.commit()

        return data
    except Exception as e:
        return get_exception_response(e)
    
