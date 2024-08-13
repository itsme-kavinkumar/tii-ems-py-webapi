from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from log_file import createFolder

def save_energy_dtl(cnx,obj):
    try:

        if obj !='':
            obj_data = json.loads(obj)
            for row in obj_data:
                id = row["id"]
                machine_id = row["machine_id"]  
                mill_date = row["mill_date"]
                initial_kwh = row["initial_kwh"]
                shift1_kwh = row["shift1_kwh"]
                shift2_kwh = row["shift2_kwh"]
                shift3_kwh = row["shift3_kwh"]
                user_login_id = row["user_login_id"]
                if id == '':
                    sql = text(f'''insert into ems_v1.dbo.energy (machine_id,mill_date,initial_kwh,shift1_kwh,shift2_kwh,shift3_kwh,created_on,created_by)
                              values('{machine_id}','{mill_date}','{initial_kwh}','{shift1_kwh}','{shift2_kwh}','{shift3_kwh}',getdate(), '{user_login_id}')''')
                else:
                    sql = text(f'''update ems_v1.dbo.energy set mill_date = '{mill_date}', initial_kwh = '{initial_kwh}',
                              shift1_kwh = '{shift1_kwh}', shift2_kwh = '{shift2_kwh}', shift3_kwh = '{shift3_kwh}', modified_on = getdate(),
                              modified_by = '{user_login_id}',machine_id = '{machine_id}'
                              where id = '{id}' ''')
                cnx.execute(sql)
                cnx.commit()
                       
    except Exception as e:
        return get_exception_response(e)
    
