from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from src.models.image import parse_date
from datetime import date
from log_file import createFolder
def savemanual_entry(cnx,obj,reportfor):
    try:
        data = 1
        mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}
        completed_db="[ems_v1_completed].[dbo]."    
        
        report = ''
        if reportfor == '12to12':
            report =f'_12'
        where = ''
        if obj != '':
            user_dict = json.loads(obj)
            for i in user_dict:
                machine_id = i['machine_id']
                kWh = i['kWh']
                mill_date = i['mill_date']
                mill_shift = i['mill_shift']

                if mill_shift != '':
                    where +=f"and mill_shift = '{mill_shift}'"
                    
                month_year=f"""{mill_month[parse_date(mill_date).month]}{str(parse_date(mill_date).year)}"""
                table_name=f"  {completed_db}[power_{month_year}]"           
                query = text(f'''
                    UPDATE {table_name}{report}
                    SET kWh = {kWh}
                    WHERE machine_id = '{machine_id}' and mill_date = '{mill_date}' {where} ''')
                createFolder("Manual_Entry_Log/", "query " + str(query))
                cnx.execute(query)
                cnx.commit()

        return data
    except Exception as e:
        return get_exception_response(e)