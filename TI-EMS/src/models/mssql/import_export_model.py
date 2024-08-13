from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from src.models.image import parse_date
from datetime import date
import re
from log_file import createFolder

def import_export_dtl(cnx,machine_id,period_id,from_date,to_date,shift_id,report_for,employee_id):
    try:
        def id(machine_id):
            if machine_id !='':
                value = machine_id.split(",")
                if len(value) > 1:
                    values = tuple(value)
                    machine_id = ",".join(values)
                else:
                    machine_id = value[0]
            return machine_id
     
        machine_id = id(machine_id)
        
        mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}       
        
        where = "" 
        group_by = ''
        order_by = ''
        department_id = ''
        shed_id = ''
        machinetype_id = ''
        if machine_id == '':
            pass
        else:
            where += f" and mm.machine_id in ({machine_id})"
        if  employee_id != '':
            query = text(f'''select * from ems_v1.dbo.master_employee where employee_id = {employee_id}''')
            res = cnx.execute(query).mappings().all()
            if len(res)>0:
                for row in res:
                    department_id = row["department_id"]
                    shed_id = row["shed_id"]
                    machinetype_id = row["machinetype_id"]
                    
        if department_id !='' and department_id !=0:
            where += f" and md.department_id ={department_id}"
        if shed_id !='' and shed_id != 0:
            where += f" and ms.shed_id ={shed_id}"
        if machinetype_id !='' and machinetype_id!= 0:
            where += f" and mmt.machinetype_id ={machinetype_id}"

        query = text(f'''SELECT * FROM [ems_v1].[dbo].[master_shifts] WHERE status = 'active' ''')
        data1 = cnx.execute(query).mappings().all()
        mill_date = date.today()
        mill_shift = 0
        table_name = ''
        
        if len(data1) > 0:
           for shift_record in data1:
              mill_date = shift_record["mill_date"]
              mill_shift = shift_record["mill_shift"]  

        field_name_import = '''mill_date, mill_shift, machine_id, master_kwh as start_kwh, machine_kwh as end_kwh, kwh, 'Import' as kwh_type '''
        field_name_export = '''mill_date, mill_shift, machine_id,reverse_master_kwh as start_kwh, reverse_machine_kwh as end_kwh, reverse_kwh as kwh, 'Export' as kwh_type'''       

        if period_id == 'cur_shift':            
            table_name = f'(select {field_name_import} from [ems_v1].[dbo].[current_power] UNION All select {field_name_export} from [ems_v1].[dbo].[current_power])cp'
            where += f" and cp.mill_date = '{mill_date}' and cp.mill_shift ='{mill_shift}' "

        elif period_id == 'sel_shift' or period_id == 'sel_date':
            if from_date == '':
                return _getErrorResponseJson("date is required") 
            
            mill_date=parse_date(from_date)             
            month_year=f"""{mill_month[mill_date.month]}{str(mill_date.year)}"""
            table_name=f"[ems_v1_completed].[dbo].[power_{month_year}]" 
            where += f" and cp.mill_date = '{mill_date}' "

            table_name = f'(select {field_name_import} from {table_name} UNION All select {field_name_export} from {table_name})cp'

            if period_id == 'sel_shift':
                if shift_id == '':
                    return _getErrorResponseJson("shift is required") 
                where += f" and cp.mill_shift ='{shift_id}' " 
            
            query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'ems_v1_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'power_{month_year}'"""
            print(query)
            result_query = cnx.execute(query).mappings().all()
            if len(result_query)>0:
                pass
            else:
                return _getErrorResponseJson("power table not available...")    
   
        elif period_id == "from_to":            
            if from_date == '':
                return _getErrorResponseJson("from date is required")
            if to_date == '':
                return _getErrorResponseJson("to_date is required")  
                    
            from_date = parse_date(from_date)
            to_date =  parse_date(to_date)
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""       
        
            where += f''' and  cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            
            if shift_id != "":                
                where += f''' and cp.mill_shift = '{shift_id}' ''' 
            
            if from_date.month == to_date.month:
                query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'ems_v1_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'power_analysis_{month_year}'"""
                result_query = cnx.execute(query).mappings().all()
                print(query)
                if len(result_query) == 0:
                    return _getErrorResponseJson("analysis table not available...")    
                table_name=f"[ems_v1_completed].[dbo].[power_{month_year}]" 
                table_name = f'(select {field_name_import} from {table_name} UNION All select {field_name_export} from {table_name})cp'
            else:
                from_month = from_date.month
                to_month = to_date.month
                month_year_range = [
                f"{mill_month[month]}{str(from_date.year)}" for month in range(from_month, to_month + 1)
                ]
                union_queries_export = []
                union_queries_import = []

                for month_year in month_year_range:
                    query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'ems_v1_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'power_{month_year}'"""
                    result_query = cnx.execute(query).mappings().all()
                    print(query)
                    if len(result_query) > 0:
                        table_name = f"[ems_v1_completed].[dbo].[power_{month_year}]"
                        union_queries_export.append(f"SELECT {field_name_export} FROM {table_name}")
                        union_queries_import.append(f"SELECT {field_name_import} FROM {table_name}")

                subquery_union_import = " UNION ALL ".join(union_queries_import)
                subquery_union_export = " UNION ALL ".join(union_queries_export)
                table_name = f"({subquery_union_import} union all {subquery_union_export}) cp"
    
        if report_for == 'detail' or report_for == '':
            group_by = " ,cp.mill_date, cp.mill_shift"
            order_by = " ,cp.mill_date, cp.mill_shift" 
        
        if report_for == 'summary':
            group_by = " ,cp.mill_date" 
            order_by = " ,cp.mill_date"   
                       
        if group_by != "":
            group_by = f"{group_by} "    
        if order_by != "":
            order_by = f"{order_by}"

        query = text(f'''
                select 
                    min(cp.mill_date) mill_date,
                    min(cp.mill_shift) mill_shift,
                    min(cp.machine_id) machine_id,
                    min(mm.machine_name) machine_name,
                    min(mm.machine_code) machine_code,
                    cp.kwh_type,
                    ROUND(SUM(
                        case when cp.kwh_type = 'Export' then
                            case when mmf.reverse_machine_kwh = '*' then cp.start_kwh * mmf.reverse_machine_kwh_value when  mmf.reverse_machine_kwh = '/' then cp.start_kwh / mmf.reverse_machine_kwh_value else cp.start_kwh end 
                        else
                            case when mmf.machine_kwh = '*' then cp.start_kwh * mmf.machine_kwh_value when  mmf.machine_kwh = '/' then cp.start_kwh / mmf.machine_kwh_value else cp.start_kwh end 
                        end
                     ),2) AS start_kwh,
                    ROUND(SUM(
                        case when cp.kwh_type = 'Export' then
                            case when mmf.reverse_machine_kwh = '*' then cp.end_kwh * mmf.reverse_machine_kwh_value when  mmf.reverse_machine_kwh = '/' then cp.end_kwh / mmf.reverse_machine_kwh_value else cp.end_kwh end
                        else
                            case when mmf.machine_kwh = '*' then cp.end_kwh * mmf.machine_kwh_value when  mmf.machine_kwh = '/' then cp.end_kwh / mmf.machine_kwh_value else cp.end_kwh end
                        end
                     ),2) AS end_kwh,
                    ROUND(SUM(
                        case when cp.kwh_type = 'Export' then
                            case when mmf.reverse_kwh = '*' then cp.kWh * mmf.reverse_kwh_value when  mmf.reverse_kwh = '/' then cp.kWh / mmf.reverse_kwh_value else cp.kWh end 
                        else
                            case when mmf.kwh = '*' then cp.kWh * mmf.kwh_value when  mmf.kwh = '/' then cp.kWh / mmf.kwh_value else cp.kWh end 
                        end
                     ),2) AS kwh
                     
                from
                    {table_name}
                    INNER JOIN [ems_v1].[dbo].[master_machine] mm ON cp.machine_id = mm.machine_id and mm.import_export = 'yes'
                    left JOIN [ems_v1].[dbo].[master_machine_factor] mmf ON mm.machine_id = mmf.machine_id  
                    LEFT JOIN [ems_v1].[dbo].[master_shed] ms ON ms.shed_id = mm.shed_id                   
                    LEFT JOIN [ems_v1].[dbo].[master_department] md ON md.department_id = mm.department_id                   
                    LEFT JOIN [ems_v1].[dbo].[master_machinetype] mmt ON mmt.machinetype_id = mm.machinetype_id 
                    where 1=1 
                    {where}
                    group by mm.machine_id {group_by}, cp.kwh_type
                    order by mm.machine_id {order_by}
                    ''')
   
        createFolder("Log/","current_power_ie api query "+str(query))

        data = cnx.execute(query).mappings().all()
        return data
    except Exception as e:
        return get_exception_response(e)