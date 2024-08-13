from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from log_file import createFolder
import json

def power_report_name(cnx):
    try:
        query= text(f'''select * from [ems_v1].[dbo].[power_report] where status = 'active' ''')
        data = cnx.execute(query).mappings().all()        
        return data
    except Exception as e:
        return get_exception_response(e)
    
def power_report_fields(cnx,company_id):
    try:
        query = text(f'''
                    SELECT 
                        min(report_field_id) as report_field_id,
                        min(report_id) as report_id,
                        min(field_code) as field_code,
                        min(field_name) as field_name,
                        min(is_show) as is_show,
                        min(slno) as slno,
                        min(field_name_display) as field_name_display,
                        min(unit) as unit,
                        min(company_id) as company_id
                    FROM 
                        [ems_v1].[dbo].[power_report_fields_original] 
                    WHERE company_id = {company_id} 
                    group by 
                        field_code 
                    order by
                         slno
                     ''')
        data = cnx.execute(query).mappings().all()        
        return data
    except Exception as e:
        return get_exception_response(e)
    
def power_reportfield(cnx,company_id,report_id):

    query = text(f'''SELECT * FROM [ems_v1].[dbo].[power_report_fields_original] where report_id = {report_id} and company_id = {company_id} order by slno''')
    result = cnx.execute(query).mappings().all()
    
    return result

def update_power_report(cnx,company_id,report_id,obj):
    try: 
        query = text(f'''UPDATE [ems_v1].[dbo].[power_report_fields_original] 
                                SET is_show = 'no' 
                                WHERE company_id = {company_id} and report_id = '{report_id}' ''')
        print(query)
        cnx.execute(query)
        cnx.commit()  
        
        if obj !='':
            obj_data = json.loads(obj)
            for row in obj_data:
                field_name_display = row["field_name_display"]
                report_field_id = row["report_field_id"]
                slno = row["slno"]
                field_code = row["field_code"]
                unit = row["field_unit"]
                
                if report_id == 0:                       
                    
                    sql = text(f'''UPDATE [ems_v1].[dbo].[power_report_fields_original] 
                                   SET field_name_display = '{field_name_display}', slno = '{slno}', unit = '{unit}'
                                   WHERE company_id = {company_id} and field_code = '{field_code}' ''')
                
                else:
                    sql = text(f'''UPDATE [ems_v1].[dbo].[power_report_fields_original] 
                                   SET is_show = 'yes', field_name_display = '{field_name_display}', slno = '{slno}' 
                                   WHERE company_id = {company_id} and report_field_id = {report_field_id} ''')
                    print(sql)
                    
                cnx.execute(sql)
                cnx.commit()
        return company_id
    
    except Exception as e:
        return get_exception_response(e)
    
