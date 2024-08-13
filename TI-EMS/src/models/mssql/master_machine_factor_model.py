from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from log_file import createFolder

def machine_factor_list(cnx,id,company_id,department_id,shed_id,machinetype_id,machine_id):
    try:
        where = ''
        if company_id != '':
            where += f'and  mc.company_id = {company_id}'

        if department_id != '':
            where += f'and  md.department_id = {department_id}'

        if shed_id != '':
            where += f'and  ms.shed_id = {shed_id}'

        if machinetype_id != '':
            where += f'and  mmt.machinetype_id = {machinetype_id}'
            
        if machine_id != '':
            where += f'and  mf.machine_id = {machine_id}'
        
        if id != '':
            where += f'and  mf.id = {id}'

        sql = text(f'''
                select 
                   mf.*,
                   mm.machine_code,
                   mm.machine_name
                from 
                   ems_v1.dbo.master_machine_factor mf,
                   ems_v1.dbo.master_machine mm
                   left join [ems_v1].[dbo].[master_company] mc on mc.company_id=mm.company_id
                   left join [ems_v1].[dbo].[master_branch] mb on mb.branch_id=mm.branch_id
                   left join [ems_v1].[dbo].[master_department] md on md.department_id=mm.department_id
                   left join [ems_v1].[dbo].[master_shed] ms on ms.shed_id=mm.shed_id
                   left join [ems_v1].[dbo].[master_machinetype] mmt on mmt.machinetype_id=mm.machinetype_id
           
                where mf.machine_id = mm.machine_id {where}''')
        print(sql)
        data = cnx.execute(sql).mappings().all()
        
        return data
    except Exception as e:
        return get_exception_response(e)
    

def update_machine_factor(cnx,machine_id,obj):
    try:
        obj_data = json.loads(obj)
        sel = {}
        for data in obj_data:
            for key, value in data.items():
                sel[key] = value
            sql = text(f'''UPDATE ems_v1.dbo.master_machine_factor SET {', '.join([f"{key} = '{value}'" for key, value in sel.items()])} WHERE machine_id = '{machine_id}' ''')
            cnx.execute(sql)
            cnx.commit()
            createFolder("Log/","data.. "+str(sql))
    except Exception as e:
        return get_exception_response(e)
    
    
