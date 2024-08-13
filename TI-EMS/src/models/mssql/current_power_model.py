from sqlalchemy import text
from src.endpoints.response_json import _getErrorResponseJson,get_exception_response
from src.models.image import save_image,parse_date,id
from log_file import createFolder
from datetime import datetime, date, timedelta
import re

def current_power_dtl(cnx, company_id ,bu_id ,plant_id ,plant_department_id ,equipment_group_id ,function_id ,meter_id ,group_for ,groupby ,period_id,from_date,to_date,shift_id,limit_report_for,limit_exception_for,limit_order_by ,limit_operation_value ,is_critical ,converter_id ,report_for,is_function , function_type ,reportfor,employee_id ,is_minmax):
    try:
             
        meter_id = id(meter_id)
        company_id = id(company_id)
        plant_id = id(plant_id)
        plant_department_id = id(plant_department_id)
        equipment_group_id = id(equipment_group_id)
        function_id = id(function_id)
        
        mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}
        completed_db="[ems_v1_completed].[dbo]."           
        where = "" 
        group_by = ""
        order_by = ""  
        function_where = ''

        if  employee_id != '':
            query = text(f'''select * from ems_v1.dbo.master_employee where employee_id = {employee_id}''')
            res = cnx.execute(query).mappings().all()

            if len(res)>0:
                for row in res:
                    plant_id = row["plant_id"]
                    plant_department_id = row["plant_department_id"]
                    equipment_group_id = row["equipment_group_id"]

        if  company_id == '' or company_id == "0":
            pass
        else:
            where += f" and  mm.company_id in ({company_id})" 
    
        if  bu_id == '' or bu_id == "0":
            pass
        else:
            where += f" and  mm.bu_id in ({bu_id})" 
    
        if plant_id == '' or plant_id == "0":
            pass
        else:
            where += f" and  mm.plant_id in ({plant_id})"          
            
        if plant_department_id == '' or plant_department_id == "0":
            pass
        else:
            where += f" and mm.plant_department_id in ({plant_department_id})"
            
        if equipment_group_id == '' or equipment_group_id == "0":
            pass
        else:
            where += f" and mm.equipment_group_id in ({equipment_group_id})"
            
        if function_id == '':
            pass
        else:
            if function_type =='':
                where += f"and  mm.function_id in ({function_id})"
            else:
                if function_type == 'function_1':
                    where += f"and  mm.function_id in ({function_id})"
                else:
                    where += f"and  mm.function2_id in ({function_id})"

        if meter_id == '' or meter_id == 'all':
            pass
        else:
            where += f" and mm.meter_id in ({meter_id})"
            
        if converter_id == '':
            pass
        else:
            where += f" and mm.converter_id = {converter_id}"
        
        if function_type !='':
            where += f" and mf.function_type = '{function_type}'"
            if function_type == 'function_2':
                function_where = f" mm.function2_id = mf.function_id"  
            else:
                function_where = f" mm.function_id = mf.function_id"
        else:
            function_where += f" mm.function_id = mf.function_id"
            
        query = text(f'''SELECT * FROM [ems_v1].[dbo].[master_shifts] WHERE status = 'active' ''')
        data1 = cnx.execute(query).mappings().all()

        mill_date = date.today()
        mill_shift = 0
        no_of_shifts = 3

        group_id = ""
        group_code = ""
        group_name = ""
        month_year = ""
        table_name = ''
        result_query = ''

        if len(data1) > 0:
           for shift_record in data1:
              mill_date = shift_record["mill_date"]
              mill_shift = shift_record["mill_shift"]  
              no_of_shifts = shift_record["no_of_shifts"]       
        
        if reportfor == '12to12':
            if period_id != 'sel_date' and period_id != 'from_to':
                return _getErrorResponseJson("invalid period id") 
            
            if period_id == "sel_date":            
                
                from_date = parse_date(from_date)
                month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
                query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'ems_v1_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'power_{month_year}_12'"""
                result_query = cnx.execute(query).mappings().all()
                if len(result_query) == 0:
                    return _getErrorResponseJson("12to12 table not available...")    
                
                table_name=f"  {completed_db}[power_{month_year}_12] as cp "       
                where += f''' and cp.mill_date = '{from_date}' '''

            elif period_id == "from_to":            
                
                        
                from_date = parse_date(from_date)
                to_date =  parse_date(to_date)
                month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""       
            
                where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' ''' 
                if from_date.month == to_date.month:
                    query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'ems_v1_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'power_{month_year}_12'"""
                    result_query = cnx.execute(query).mappings().all()
                    if len(result_query) == 0:
                       return _getErrorResponseJson("12to12 table not available...")    
                
                    table_name=f"  {completed_db}[power_{month_year}_12] as cp "
                else:
                    field_name = 'power_id,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,meter_id,design_id,beam_id,date_time,date_time1,mill_date,mill_shift,vln_avg,r_volt,y_volt,b_volt,vll_avg,ry_volt,yb_volt,br_volt,t_current,r_current,y_current,b_current,t_watts,r_watts,y_watts,b_watts,t_var,r_var,y_var,b_var,t_voltampere,r_voltampere,y_voltampere,b_voltampere,avg_powerfactor,r_powerfactor,y_powerfactor,b_powerfactor,powerfactor,kWh,kvah,kw,kvar,power_factor,kva,frequency,machine_status,status,created_on,created_by,modified_on,modified_by,machine_kWh,master_kwh,reverse_machine_kwh,reverse_master_kwh,reverse_kwh'
                    
                    from_month = from_date.month
                    to_month = to_date.month
                    month_year_range = [
                        (from_date + timedelta(days=30 * i)).strftime("%m%Y")
                        for i in range((to_date.year - from_date.year) * 12 + to_date.month - from_date.month + 1)
                    ]

                    union_queries = []

                    for month_year in month_year_range:
                        query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'ems_v1_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'power_{month_year}_12'"""
                        result_query = cnx.execute(query).mappings().all()
                        print(query)
                        if len(result_query) > 0:
                            table_name = f"[ems_v1_completed].[dbo].[power_{month_year}_12]"
                            union_queries.append(f"SELECT {field_name} FROM {table_name}")
                    if len(union_queries) == 0:
                       return _getErrorResponseJson("12to12 table not available...")    
                
                    subquery_union = " UNION ALL ".join(union_queries)
                    table_name = f"( {subquery_union}) cp"
        
        else:

            if period_id == "cur_shift": 
                      
                where += f''' and cp.mill_date = '{mill_date}' AND cp.mill_shift = '{mill_shift}' '''              
                table_name = "[ems_v1].[dbo].[current_power] cp"  

            elif period_id == "#cur_shift":
                where += f''' and cp.mill_date = '{mill_date}' AND cp.mill_shift = '{mill_shift}' '''              
                table_name = "[ems_v1].[dbo].[current_power] cp" 

            elif period_id == "sel_shift":                  
                
             
                from_date = parse_date(from_date)          
                month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
                table_name=f"  {completed_db}[power_{month_year}] as cp" 

                query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'ems_v1_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'power_{month_year}'"""
                result_query = cnx.execute(query).mappings().all()
                print(query)
                if len(result_query) == 0:
                    return _getErrorResponseJson("power table not available...")    
                
                where += f''' and cp.mill_date = '{from_date}' AND cp.mill_shift = '{shift_id}' '''   

            elif period_id == "#sel_shift":                 
                if mill_shift == 1:
                    shift_id = no_of_shifts
                    from_date = parse_date(mill_date) - timedelta(days=1)

                else:
                    shift_id = int(mill_shift) - 1
                    from_date = mill_date                      
                month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
                table_name=f"  {completed_db}[power_{month_year}] as cp" 

                query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'ems_v1_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'power_{month_year}'"""
                result_query = cnx.execute(query).mappings().all()
                print(query)
                if len(result_query) == 0:
                    return _getErrorResponseJson("power table not available...")    
                
                where += f''' and cp.mill_date = '{from_date}' AND cp.mill_shift = '{shift_id}' '''   
            
            elif period_id == "sel_date":            
                if from_date == '':
                    return _getErrorResponseJson("from date is required")    
                
                from_date = parse_date(from_date)
                
                month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
                table_name=f"  {completed_db}[power_{month_year}] as cp "       
                where += f''' and cp.mill_date = '{from_date}' '''

                query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'ems_v1_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'power_{month_year}'"""
                result_query = cnx.execute(query).mappings().all()
                print(query)
                if len(result_query) == 0:
                    return _getErrorResponseJson("power table not available...")    
                
            elif period_id == "#sel_date":             
                from_date = mill_date
                # from_date = parse_date(from_date)
                month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
                table_name=f"  {completed_db}[power_{month_year}] as cp "
                where += f''' and cp.mill_date = '{from_date}' '''
                field_name = 'power_id,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,meter_id,design_id,beam_id,date_time,date_time1,mill_date,mill_shift,vln_avg,r_volt,y_volt,b_volt,vll_avg,ry_volt,yb_volt,br_volt,t_current,r_current,y_current,b_current,t_watts,r_watts,y_watts,b_watts,t_var,r_var,y_var,b_var,t_voltampere,r_voltampere,y_voltampere,b_voltampere,avg_powerfactor,r_powerfactor,y_powerfactor,b_powerfactor,powerfactor,kWh,kvah,kw,kvar,power_factor,kva,frequency,machine_status,status,created_on,created_by,modified_on,modified_by,machine_kWh,master_kwh,reverse_machine_kwh,reverse_master_kwh,reverse_kwh'
                    
                query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'ems_v1_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'power_{month_year}'"""
                result_query = cnx.execute(query).mappings().all()
                table_name = f'''(select {field_name} from ems_v1.dbo.current_power union all select {field_name} from ems_v1_completed.dbo.power_{month_year}) as cp'''

            elif period_id == "#previous_date":             
                from_date = mill_date - timedelta(days=1)
                # from_date = parse_date(from_date)
                
                month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
                table_name=f"  {completed_db}[power_{month_year}] as cp "
                where += f''' and cp.mill_date = '{from_date}' '''

                query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'ems_v1_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'power_{month_year}'"""
                result_query = cnx.execute(query).mappings().all()
                print(query)
                if len(result_query) == 0:
                    return _getErrorResponseJson("power table not available...")    
                
            elif period_id  == "#this_week":

                dt = mill_date
                from_date=dt-timedelta(dt.weekday()+1)
                to_date = mill_date
                month_year = f"{mill_month[from_date.month]}{str(from_date.year)}"
                where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''

            elif period_id == "#previous_week":

                dt = mill_date
                current_week_start = dt - timedelta(days=dt.weekday())  
                from_date = current_week_start - timedelta(weeks=1)  
                to_date = from_date + timedelta(days=5)
                month_year = f"{mill_month[from_date.month]}{str(from_date.year)}"
                where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''

            elif period_id == "#this_month":
                from_date = mill_date.replace(day=1)
                to_date = mill_date

                month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""            
                table_name=f"  {completed_db}[power_{month_year}] as cp "
                where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''

                query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'ems_v1_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'power_{month_year}'"""
                result_query = cnx.execute(query).mappings().all()
                print(query)
                if len(result_query) == 0:
                    return _getErrorResponseJson("power table not available...")
                    
            elif period_id == "#previous_month":
                from_date = mill_date.replace(day=1)
                # Calculate the first day of the previous month
                from_date = (from_date - timedelta(days=1)).replace(day=1)
                to_date = from_date + timedelta(days=30) 
                month_year = f"{mill_month[from_date.month]}{str(from_date.year)}"
                where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
                
            elif period_id=="#this_year": 
                
                from_date = mill_date.replace(day=1,month=1) 
                to_date = mill_date  
                month_year = f"{mill_month[from_date.month]}{str(from_date.year)}"
                where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''

            elif period_id=="#previous_year": 
                
                from_date = mill_date.replace(day=1, month=1, year=mill_date.year - 1)
                to_date = from_date.replace(day=1, month=12) + timedelta(days=30)
                print("from_date",from_date)
                print("to_date",to_date)
                month_year = f"{mill_month[from_date.month]}{str(from_date.year)}"
                where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''

            elif period_id == "from_to":            
                
                from_date = parse_date(from_date)
                to_date =  parse_date(to_date)
                month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""       
                where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''

                if shift_id != "" and shift_id !="all":                
                    where += f''' and cp.mill_shift = '{shift_id}' ''' 
            else:
                return _getErrorResponseJson("invalid period id") 
            
            if period_id == '#previous_week' or period_id == "#this_week" or period_id == "#this_month" or period_id == '#previous_month' or period_id=="#previous_year" or period_id=="#this_year" or period_id=="from_to":
                if from_date != '' and to_date != '':
                    if from_date.month == to_date.month and from_date.year == to_date.year:

                        query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'ems_v1_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'power_{month_year}'"""
                        result_query = cnx.execute(query).mappings().all()
                        print(query)
                        if len(result_query) == 0:
                            return _getErrorResponseJson("power table not available...")    
                    
                        table_name=f"  {completed_db}[power_{month_year}] as cp "
                    else:
                        field_name = 'power_id,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,meter_id,design_id,beam_id,date_time,date_time1,mill_date,mill_shift,vln_avg,r_volt,y_volt,b_volt,vll_avg,ry_volt,yb_volt,br_volt,t_current,r_current,y_current,b_current,t_watts,r_watts,y_watts,b_watts,t_var,r_var,y_var,b_var,t_voltampere,r_voltampere,y_voltampere,b_voltampere,avg_powerfactor,r_powerfactor,y_powerfactor,b_powerfactor,powerfactor,kWh,kvah,kw,kvar,power_factor,kva,frequency,machine_status,status,created_on,created_by,modified_on,modified_by,machine_kWh,master_kwh,reverse_machine_kwh,reverse_master_kwh,reverse_kwh'
                            
                        month_year_range = [
                                (from_date + timedelta(days=30 * i)).strftime("%m%Y")
                                for i in range((to_date.year - from_date.year) * 12 + to_date.month - from_date.month + 1)
                            ]
                            # else:
                            #     month_year_range = [
                            #     f"{mill_month[month]}{str(from_date.year)}" for month in range(from_month, to_month + 1)
                            #     ]
                        print("month_year_range",month_year_range)
                        union_queries = []

                        for month_year in month_year_range:
                            query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'ems_v1_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'power_{month_year}'"""
                            result_query = cnx.execute(query).mappings().all()
                            print(query)
                            if len(result_query) > 0:
                                table_name = f"[ems_v1_completed].[dbo].[power_{month_year}]"
                                union_queries.append(f"SELECT {field_name} FROM {table_name}")

                        if len(union_queries) == 0:
                            return _getErrorResponseJson("power table not available...")    
                        
                        subquery_union = " UNION ALL ".join(union_queries)
                        table_name = f"( {subquery_union}) cp"

        order_by_limit = ''
        if limit_report_for == "exception" :
           
            order_by_limit += f"{limit_exception_for}"
            if limit_order_by == "asc":
                order_by_limit += " "+limit_order_by +","  
            else:
                order_by_limit += " "+limit_order_by +","

        if groupby != '' and groupby == "company":
            group_by += " mm.company_id "
            order_by += " mm.company_id "
            group_id = '''min(mc.company_id) AS group_id '''
            group_code = '''min(mc.company_code) AS group_code ,'''
            group_name = '''min(mc.company_name) AS group_name'''       
            
        if groupby != '' and groupby == "branch":
            group_by += " mm.bu_id "
            order_by += " mm.bu_id "
            group_id = '''min(mb.bu_id) AS group_id '''
            group_code = '''min(mb.branch_code) AS group_code ,'''
            group_name = '''min(mb.branch_name) AS group_name'''       
            
        if groupby !='' and groupby == "department":
            group_by += " mm.plant_id "
            order_by += " min(md.department_order)"
            group_id = '''min(md.plant_id) AS group_id '''
            group_code = '''min(md.department_code) AS group_code ,'''
            group_name = '''min(md.department_name) AS group_name'''        
            
        if groupby !='' and groupby == "shed":
            group_by += "  mm.plant_department_id "
            order_by += "  min(ms.shed_order)"
            group_id = ''' min(ms.plant_department_id) AS group_id '''
            group_code = ''' min(ms.shed_code) AS group_code ,'''
            group_name = ''' min(ms.shed_name) AS group_name'''
            
        if groupby !='' and groupby == "machinetype":
            group_by += " mm.equipment_group_id"
            order_by += " min(mmt.machinetype_order)"
            group_id = '''min(mmt.equipment_group_id) AS group_id '''
            group_code = '''min(mmt.machinetype_code) AS group_code ,'''
            group_name = '''min(mmt.machinetype_name) AS group_name'''
            
        if groupby !='' and groupby == "function":    
            order_by += " min(mf.function_order)"
            group_id = '''min(mf.function_id) AS group_id '''
            group_code = '''min(mf.function_code) AS group_code ,'''
            group_name = '''min(mf.function_name) AS group_name''' 

            if function_type !='':
                if function_type == 'function_1':
                    group_by += " mm.function_id" 
                else:
                    group_by += " mm.function2_id"     
            else:
                group_by += " mm.function_id"

            if is_function !="":
                group_by += " ,mm.meter_id"
                order_by += " ,mm.meter_id"

        if groupby !='' and groupby == "converter":           
            group_by += " mm.converter_id"
            order_by += " mm.converter_id"
            group_id = '''min(mcd.converter_id) AS group_id '''
            group_code = ''
            group_name = '''min(mcd.converter_name) AS group_name'''

            if is_function !="":
                group_by += " ,mm.meter_id"
                order_by += " ,mm.meter_id"
            
        if groupby !='' and groupby == "machine":             
            group_by += " mm.meter_id"
            order_by += " min(mm.machine_order)"
            group_id = '''min(mm.meter_id) AS group_id '''
            group_code = '''min(mm.machine_code) AS group_code ,'''
            group_name = '''min(mm.machine_name) AS group_name'''  
           
        if limit_operation_value !='' and limit_operation_value != '0':          
            order_by += " " +"OFFSET 0 ROWS FETCH NEXT"+' '+ limit_operation_value+" "+"ROWS ONLY"
    
        if is_critical == "yes" or is_critical == "no"  :
            where += f" and mm.major_nonmajor = '{is_critical}' "   
            
        where_group_for = ""  
        # if group_for == "exception" and meter_id != 'all' and meter_id!= "":
        if group_for == "exception":
            if groupby == "company":
                where_group_for += "and group_type = 'company' " 
                if company_id != '' and company_id != '0':
                    where_group_for += f"and type_id = '{company_id}'"

            if groupby == "department":
                where_group_for += "and group_type = 'zone' " 
                if plant_id != 'all' and plant_id !='' and plant_id != '0':
                    where_group_for += f"and type_id = '{plant_id}'"
                    
            if groupby == "shed":
                where_group_for += "and group_type = 'area' "
                if plant_department_id != 'all' and plant_department_id !='' and plant_department_id != '0':
                    where_group_for += f"and type_id = '{plant_department_id}'"
                    
            if groupby == "machinetype":
                where_group_for += "and group_type = 'location' "
                if equipment_group_id != 'all' and equipment_group_id !='' and equipment_group_id != '0':
                    where_group_for += f"and type_id = '{equipment_group_id}'"
            
            if groupby == "function":
                where_group_for += "and group_type = 'function' "
                if function_id != 'all' and function_id !='' and function_id != '0':
                    where_group_for += f"and type_id = '{function_id}'"
                    
            sql = text(f'''SELECT * FROM [ems_v1].[dbo].[master_meter_group] where 1=1 {where_group_for} ''') 
            data2 = cnx.execute(sql).mappings().all()
            meter_id = []  
            
            if len(data2) > 0:
                for record in data2:
                    meter_id.append(record["meter_id"]) 
                    
                where += f" and mm.meter_id in ({','.join(str(x) for x in meter_id)})"

        if report_for == 'detail' or report_for == '':
            group_by = " cp.mill_date , cp.mill_shift," + group_by
            order_by = " cp.mill_date, cp.mill_shift," + order_by
        
        if report_for == 'summary':
            group_by = " cp.mill_date," + group_by
            order_by = " cp.mill_date," + order_by  

        if group_by != "":
            group_by = f"group by {group_by} "    
        if order_by != "":
            order_by = f"order by {order_by_limit}{order_by}"

        query1 = ''
        query2 = ''

        if is_minmax == 'yes':
            query1 += f'WITH query AS ( '
            query2 += f''')
                SELECT
                    ROUND(MIN(kwh),2) AS kwh_min,
                    ROUND(MAX(kwh),2) AS kwh_max,
                    ROUND(AVG(kwh),2) AS avg_kWh,
                    ROUND(SUM(kWh),2) AS total_kWh,
                    min(meter_id) as meter_id,  
                    min(machine_name) as machine_name,
                    min(machine_code) as machine_code,
                    min(plant_id) as plant_id,
                    min(department_name) as department_name,
                    min(department_code) as department_code,
                    min(plant_department_id) as plant_department_id,
                    min(shed_name) as shed_name,
                    min(shed_code) as shed_code,
                    min(equipment_group_id) as equipment_group_id,
                    min(machinetype_name) as machinetype_name,
                    min(machinetype_code) as machinetype_code,
                    (SELECT TOP 1 mill_date FROM query ORDER BY kwh ASC) AS min_date,
                    (SELECT TOP 1 mill_date FROM query ORDER BY kwh DESC) AS max_date,
                    (SELECT TOP 1 mill_shift FROM query ORDER BY kwh ASC) AS min_shift,
                    (SELECT TOP 1 mill_shift FROM query ORDER BY kwh DESC) AS max_shift
                FROM query 
                 '''
            order_by = ''

    #    ROUND(sum(CASE WHEN mm.energy_selection = 'wh' THEN CAST(cp.kWh AS DECIMAL(18, 2))/1000 ELSE cp.kWh END),2) AS kWh,
        query = text(f'''{query1}
                SELECT                       
                    min(mc.company_code) AS company_code,
                    min(mc.company_name) AS company_name,
                    min(mb.branch_code) AS branch_code,
                    min(mb.branch_name) AS branch_name,
                    min(md.department_code) AS department_code,
                    min(md.department_name) As department_name,
                    min(ms.shed_code) AS shed_code,
                    min(ms.shed_name) AS shed_name,
                    min(mmt.machinetype_code) AS machinetype_code,
                    min(mmt.machinetype_name) AS machinetype_name,
                    min(mf.function_name) AS function_name,
                    min(mf.function_code) AS function_code,
                    min(mm.machine_code) AS machine_code,
                    min(mm.machine_name) AS machine_name,
                    count(mm.machine_name) AS machine_count,
                    min(cp.power_id) as power_id,
                    min(cp.company_id) as company_id,
                    min(cp.bu_id) as bu_id,
                    min(cp.plant_id) as plant_id,
                    min(cp.plant_department_id) as plant_department_id,
                    min(cp.equipment_group_id) as equipment_group_id,
                    min(mf.function_id) AS function_id,
                    min(cp.meter_id) as meter_id,
                    min(cp.design_id) as design_id,
                    min(cp.beam_id) as beam_id,
                    min(cp.date_time) as date_time,
                    min(cp.date_time1) as date_time1,
                    min(cp.mill_date) as mill_date,
                    min(cp.mill_shift) as mill_shift,
                    ROUND(SUM(case when mmf.vln_avg = '*' then cp.vln_avg * mmf.vln_avg_value when  mmf.vln_avg = '/' then cp.vln_avg / mmf.vln_avg_value else cp.vln_avg end ),min(prf.vln_avg)) AS vln_avg,
                    ROUND(SUM(case when mmf.r_volt = '*' then cp.r_volt * mmf.r_volt_value when  mmf.r_volt = '/' then cp.r_volt / mmf.r_volt_value else cp.r_volt end ),min(prf.r_volt)) AS r_volt,
                    ROUND(SUM(case when mmf.y_volt = '*' then cp.y_volt * mmf.y_volt_value when  mmf.y_volt = '/' then cp.y_volt / mmf.y_volt_value else cp.y_volt end ),min(prf.y_volt)) AS y_volt,
                    ROUND(SUM(case when mmf.b_volt = '*' then cp.b_volt * mmf.b_volt_value when  mmf.b_volt = '/' then cp.b_volt / mmf.b_volt_value else cp.b_volt end ),min(prf.b_volt)) AS b_volt,
                    ROUND(SUM(case when mmf.vll_avg = '*' then cp.vll_avg * mmf.vll_avg_value when  mmf.vll_avg = '/' then cp.vll_avg / mmf.vll_avg_value else cp.vll_avg end ),min(prf.vll_avg)) AS vll_avg,
                    ROUND(SUM(case when mmf.ry_volt = '*' then cp.ry_volt * mmf.ry_volt_value when  mmf.ry_volt = '/' then cp.ry_volt / mmf.ry_volt_value else cp.ry_volt end ),min(prf.ry_volt)) AS ry_volt,
                    ROUND(SUM(case when mmf.yb_volt = '*' then cp.yb_volt * mmf.yb_volt_value when  mmf.yb_volt = '/' then cp.yb_volt / mmf.yb_volt_value else cp.yb_volt end ),min(prf.yb_volt)) AS yb_volt,
                    ROUND(SUM(case when mmf.br_volt = '*' then cp.br_volt * mmf.br_volt_value when  mmf.br_volt = '/' then cp.br_volt / mmf.br_volt_value else cp.br_volt end ),min(prf.br_volt)) AS br_volt,
                    ROUND(SUM(case when mmf.r_current = '*' then cp.r_current * mmf.r_current_value when  mmf.r_current = '/' then cp.r_current / mmf.r_current_value else cp.r_current end ),min(prf.r_current)) AS r_current,
                    ROUND(SUM(case when mmf.y_current = '*' then cp.y_current * mmf.y_current_value when  mmf.y_current = '/' then cp.y_current / mmf.y_current_value else cp.y_current end ),min(prf.y_current)) AS y_current,
                    ROUND(SUM(case when mmf.b_current = '*' then cp.b_current * mmf.b_current_value when  mmf.b_current = '/' then cp.b_current / mmf.b_current_value else cp.b_current end ),min(prf.b_current)) AS b_current,
                    ROUND(SUM(case when mmf.t_current = '*' then cp.t_current * mmf.t_current_value when  mmf.t_current = '/' then cp.t_current / mmf.t_current_value else cp.t_current end ),min(prf.t_current)) AS t_current,
                    ROUND(SUM(case when mmf.t_watts = '*' then cp.t_watts * mmf.t_watts_value when  mmf.t_watts = '/' then cp.t_watts / mmf.t_watts_value else cp.t_watts end ),min(prf.t_watts)) AS t_watts,
                    ROUND(SUM(case when mmf.r_watts = '*' then cp.r_watts * mmf.r_watts_value when  mmf.r_watts = '/' then cp.r_watts / mmf.r_watts_value else cp.r_watts end ),min(prf.r_watts)) AS r_watts,
                    ROUND(SUM(case when mmf.y_watts = '*' then cp.y_watts * mmf.y_watts_value when  mmf.y_watts = '/' then cp.y_watts / mmf.y_watts_value else cp.y_watts end ),min(prf.y_watts)) AS y_watts,
                    ROUND(SUM(case when mmf.b_watts = '*' then cp.b_watts * mmf.b_watts_value when  mmf.b_watts = '/' then cp.b_watts / mmf.b_watts_value else cp.b_watts end ),min(prf.b_watts)) AS b_watts,
                    ROUND(SUM(case when mmf.t_var = '*' then cp.t_var * mmf.t_var_value when  mmf.t_var = '/' then cp.t_var / mmf.t_var_value else cp.t_var end ),min(prf.t_var)) AS t_var,
                    ROUND(SUM(case when mmf.r_var = '*' then cp.r_var * mmf.r_var_value when  mmf.r_var = '/' then cp.r_var / mmf.r_var_value else cp.r_var end ),min(prf.r_var)) AS r_var,
                    ROUND(SUM(case when mmf.y_var = '*' then cp.y_var * mmf.y_var_value when  mmf.y_var = '/' then cp.y_var / mmf.y_var_value else cp.y_var end ),min(prf.y_var)) AS y_var,
                    ROUND(SUM(case when mmf.b_var = '*' then cp.b_var * mmf.b_var_value when  mmf.b_var = '/' then cp.b_var / mmf.b_var_value else cp.b_var end ),min(prf.b_var)) AS b_var,
                    ROUND(SUM(case when mmf.t_voltampere = '*' then cp.t_voltampere * mmf.t_voltampere_value when  mmf.t_voltampere = '/' then cp.t_voltampere / mmf.t_voltampere_value else cp.t_voltampere end ),min(prf.t_voltampere)) AS t_voltampere,
                    ROUND(SUM(case when mmf.r_voltampere = '*' then cp.r_voltampere * mmf.r_voltampere_value when  mmf.r_voltampere = '/' then cp.r_voltampere / mmf.r_voltampere_value else cp.r_voltampere end ),min(prf.r_voltampere)) AS r_voltampere,
                    ROUND(SUM(case when mmf.y_voltampere = '*' then cp.y_voltampere * mmf.y_voltampere_value when  mmf.y_voltampere = '/' then cp.y_voltampere / mmf.y_voltampere_value else cp.y_voltampere end ),min(prf.y_voltampere)) AS y_voltampere,
                    ROUND(SUM(case when mmf.b_voltampere = '*' then cp.b_voltampere * mmf.b_voltampere_value when  mmf.b_voltampere = '/' then cp.b_voltampere / mmf.b_voltampere_value else cp.b_voltampere end ),min(prf.b_voltampere)) AS b_voltampere,
                    ROUND(SUM(case when mmf.avg_powerfactor = '*' then cp.avg_powerfactor * mmf.avg_powerfactor_value when  mmf.avg_powerfactor = '/' then cp.avg_powerfactor / mmf.avg_powerfactor_value else cp.avg_powerfactor end ),min(prf.avg_powerfactor)) AS avg_powerfactor,
                    ROUND(SUM(case when mmf.r_powerfactor = '*' then cp.r_powerfactor * mmf.r_powerfactor_value when  mmf.r_powerfactor = '/' then cp.r_powerfactor / mmf.r_powerfactor_value else cp.r_powerfactor end ),min(prf.r_powerfactor)) AS r_powerfactor,
                    ROUND(SUM(case when mmf.y_powerfactor = '*' then cp.y_powerfactor * mmf.y_powerfactor_value when  mmf.y_powerfactor = '/' then cp.y_powerfactor / mmf.y_powerfactor_value else cp.y_powerfactor end ),min(prf.y_powerfactor)) AS y_powerfactor,
                    ROUND(SUM(case when mmf.b_powerfactor = '*' then cp.b_powerfactor * mmf.b_powerfactor_value when  mmf.b_powerfactor = '/' then cp.b_powerfactor / mmf.b_powerfactor_value else cp.b_powerfactor end ),min(prf.b_powerfactor)) AS b_powerfactor,
                    ROUND(SUM(case when mmf.powerfactor = '*' then cp.powerfactor * mmf.powerfactor_value when  mmf.powerfactor = '/' then cp.powerfactor / mmf.powerfactor_value else cp.powerfactor end ),min(prf.powerfactor)) AS powerfactor,
                    
                    ROUND(SUM(case when mmf.kvah = '*' then cp.kvah * mmf.kvah_value when  mmf.kvah = '/' then cp.kvah / mmf.kvah_value else cp.kvah end ),min(prf.kvah)) AS kvah,
                    ROUND(SUM(case when mmf.kw = '*' then cp.t_watts * mmf.kw_value when  mmf.kw = '/' then cp.t_watts / mmf.kw_value else cp.t_watts end ),min(prf.kw)) AS kw,
                    ROUND(SUM(case when mmf.kvar = '*' then cp.kvar * mmf.kvar_value when  mmf.kvar = '/' then cp.kvar / mmf.kvar_value else cp.kvar end ),min(prf.kvar)) AS kvar,
                    ROUND(SUM(case when mmf.power_factor = '*' then cp.power_factor * mmf.power_factor_value when  mmf.power_factor = '/' then cp.power_factor / mmf.power_factor_value else cp.power_factor end ),min(prf.power_factor)) AS power_factor,
                    ROUND(SUM(case when mmf.kva = '*' then cp.kva * mmf.kva_value when  mmf.kva = '/' then cp.kva / mmf.kva_value else cp.kva end ),min(prf.kva)) AS kva,
                    ROUND(SUM(case when mmf.frequency = '*' then cp.frequency * mmf.frequency_value when  mmf.frequency = '/' then cp.frequency / mmf.frequency_value else cp.frequency end ),min(prf.frequency)) AS frequency,
                    min(cp.machine_status) as machine_status,
                    min(cp.status) as status,
                    min(cp.created_on) as created_on,
                    min(cp.created_by) as created_by,
                    min(cp.modified_on) as modified_on,
                    min(cp.modified_by) as modified_by,
                    
                    ROUND(SUM(case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end ),min(prf.machine_kWh)) AS machine_kWh,
                    ROUND(SUM(case when mmf.machine_kWh = '*' then cp.master_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.master_kwh / mmf.machine_kWh_value else cp.master_kwh end ),min(prf.machine_kWh)) AS master_kwh,
                    ROUND(SUM(case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value when  mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end ),min(prf.kWh)) AS kWh,
                    ROUND(MIN(case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value when  mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end ),min(prf.kWh)) AS min_kWh,
                    ROUND(MAX(case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value when  mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end ),min(prf.kWh)) AS max_kWh,
                    ROUND(AVG(case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value when  mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end ),min(prf.kWh)) AS avg_kWh,
                     
                    ROUND(SUM(case when mmf.machine_kWh = '*' then cp.reverse_machine_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.reverse_machine_kwh / mmf.machine_kWh_value else cp.reverse_machine_kwh end ),min(prf.machine_kWh)) AS reverse_machine_kwh,
                    ROUND(SUM(case when mmf.machine_kWh = '*' then cp.reverse_master_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.reverse_master_kwh / mmf.machine_kWh_value else cp.reverse_master_kwh end ),min(prf.machine_kWh)) AS reverse_master_kwh,
                    ROUND(SUM(case when mmf.kWh = '*' then cp.reverse_kwh * mmf.kWh_value when  mmf.kWh = '/' then cp.reverse_kwh / mmf.kWh_value else cp.reverse_kwh end ),min(prf.kWh)) AS reverse_kwh,
                    
                    min(mm.ip_address) as ip_address,
                    min(mm.port) as port,
                    CASE WHEN min(cp.date_time) <= DATEADD(minute, -2, getdate()) THEN 'S' ELSE 'N' END as nocom,       
                    ROUND(SUM(CASE WHEN cp.mill_shift = 1 THEN case when mmf.kWh = '*' then cp.kwh * mmf.kwh_value when  mmf.kwh = '/' then cp.kwh / mmf.kwh_value else cp.kwh end ELSE 0 END),min(prf.kwh)) AS kwh_1,
                    ROUND(SUM(CASE WHEN cp.mill_shift = 2 THEN case when mmf.kWh = '*' then cp.kwh * mmf.kwh_value when  mmf.kwh = '/' then cp.kwh / mmf.kwh_value else cp.kwh end ELSE 0 END),min(prf.kwh)) AS kwh_2,
                    ROUND(SUM(CASE WHEN cp.mill_shift = 3 THEN case when mmf.kWh = '*' then cp.kwh * mmf.kwh_value when  mmf.kwh = '/' then cp.kwh / mmf.kwh_value else cp.kwh end ELSE 0 END),min(prf.kwh)) AS kwh_3,
                    ROUND(SUM(CASE WHEN cp.mill_shift = 1 THEN case when mmf.machine_kwh = '*' then cp.master_kwh * mmf.machine_kwh_value when  mmf.machine_kwh = '/' then cp.master_kwh / mmf.machine_kwh_value else cp.master_kwh end ELSE 0 END),min(prf.machine_kWh)) AS start_kwh_1,
                    ROUND(SUM(CASE WHEN cp.mill_shift = 2 THEN case when mmf.machine_kwh = '*' then cp.master_kwh * mmf.machine_kwh_value when  mmf.machine_kwh = '/' then cp.master_kwh / mmf.machine_kwh_value else cp.master_kwh end ELSE 0 END),min(prf.machine_kWh)) AS start_kwh_2,
                    ROUND(SUM(CASE WHEN cp.mill_shift = 3 THEN case when mmf.machine_kwh = '*' then cp.master_kwh * mmf.machine_kwh_value when  mmf.machine_kwh = '/' then cp.master_kwh / mmf.machine_kwh_value else cp.master_kwh end ELSE 0 END),min(prf.machine_kWh)) AS start_kwh_3,     
                    ROUND(SUM(CASE WHEN cp.mill_shift = 1 THEN case when mmf.machine_kwh = '*' then cp.machine_kwh * mmf.machine_kwh_value when  mmf.machine_kwh = '/' then cp.machine_kwh / mmf.machine_kwh_value else cp.machine_kwh end ELSE 0 END),min(prf.machine_kWh)) AS end_kwh_1,
                    ROUND(SUM(CASE WHEN cp.mill_shift = 2 THEN case when mmf.machine_kwh = '*' then cp.machine_kwh * mmf.machine_kwh_value when  mmf.machine_kwh = '/' then cp.machine_kwh / mmf.machine_kwh_value else cp.machine_kwh end ELSE 0 END),min(prf.machine_kWh)) AS end_kwh_2,
                    ROUND(SUM(CASE WHEN cp.mill_shift = 3 THEN case when mmf.machine_kwh = '*' then cp.machine_kwh * mmf.machine_kwh_value when  mmf.machine_kwh = '/' then cp.machine_kwh / mmf.machine_kwh_value else cp.machine_kwh end ELSE 0 END),min(prf.machine_kWh)) AS end_kwh_3,
                    {group_id},
                    {group_code}
                    {group_name}                       
                FROM 
                    {table_name}                       
                    INNER JOIN [ems_v1].[dbo].[master_machine] mm ON cp.meter_id = mm.meter_id
                    INNER JOIN [ems_v1].[dbo].[master_company] mc ON mm.company_id = mc.company_id
                    INNER JOIN [ems_v1].[dbo].[master_branch] mb ON mm.bu_id = mb.bu_id
                    INNER JOIN [ems_v1].[dbo].[master_department] md ON mm.plant_id = md.plant_id
                    INNER JOIN [ems_v1].[dbo].[master_shed] ms ON mm.plant_department_id = ms.plant_department_id
                    INNER JOIN [ems_v1].[dbo].[master_machinetype] mmt ON mm.equipment_group_id = mmt.equipment_group_id 
                    LEFT JOIN [ems_v1].[dbo].[master_function] mf ON {function_where}
                    LEFT JOIN [ems_v1].[dbo].[master_converter_detail] mcd ON mm.converter_id = mcd.converter_id 
                    LEFT JOIN [ems_v1].[dbo].[master_machine_factor] mmf ON mm.meter_id = mmf.meter_id                   
                    LEFT JOIN [ems_v1].[dbo].[master_parameter_roundoff] prf ON mm.company_id = prf.company_id                  
                WHERE  
                    cp.status = '0' and mm.status = 'active'
                    {where}                        
                    {group_by}
                    {order_by} 
                    {query2}
                ''')
        createFolder("Current_power_log/","current_power api query "+str(query))
        data = cnx.execute(query).mappings().all()
        return data   
    
    except Exception as e:
        return get_exception_response(e)
    
def function_dashboard(cnx):
    try:
        mill_date = date.today()
        mill_shift = 0
        group_name = ''
        func_name = ''
        formula1 = ''
        results = ''

        sql1 = f'select * from ems_v1.dbo.master_shifts'
        data = cnx.execute(sql1).mappings().all()
        if len(data)>0:
            for row in data:
                mill_date = row["mill_date"]
                mill_shift = row["mill_shift"]

        sql2 = f'SELECT * FROM [ems_v1].[dbo].[master_energy_calculations] ORDER BY group_name, s_no'
        result = cnx.execute(sql2).mappings().all()
        if len(result)>0:
            para = ''
            for rows in result:
                para = rows['parameter']

            if para == 'kw':
                para = "case when mmf.kw = '*' then p.t_watts * mmf.kw_value when  mmf.kw = '/' then p.t_watts / mmf.kw_value else p.t_watts end "

            if para == 'kWh':
                para = "case when mmf.kWh = '*' then p.kWh * mmf.kWh_value when  mmf.kWh = '/' then p.kWh / mmf.kWh_value else p.kWh end "
        
            sql3 = text(f'''
                    select 
                        p.meter_id,
                        min(mm.machine_name) as machine_name,
                        sum({para}) as kWh 
                    from 
                        [ems_v1].[dbo].[current_power] p
                        left join  [ems_v1].[dbo].[master_machine] mm on mm.meter_id=p.meter_id
                        left join  [ems_v1].[dbo].[master_machine_factor] mmf on mm.meter_id=mmf.meter_id
                    where 
                        p.mill_date = '{mill_date}' and p.mill_shift = {mill_shift}
                    group by 
                        p.meter_id 
                    order by 
                        p.meter_id''')
            print(sql3)
            res = cnx.execute(sql3).mappings().all()

            meter_id_dict={}
            # dict_tt ={}
            dict={}
            for row in res:
                dict[row['meter_id']] = row['kWh']
                # dict_tt[row['machine_name']] = row['kWh']
                meter_id_dict[row['meter_id']] = row['machine_name']
            datas = []

            for rows in result:
                group_name = rows['group_name']
                func_name = rows['function_name']
                formula = rows['formula2']
                formula1 = rows['formula1']
                
                results = eval(formula, {"dict": dict})
                
                # meter_ids = re.findall(r'dict\[(\d+)\]', formula)
        
                # # Convert extracted IDs to integers and filter out IDs not present in the dict
                # valid_formula_meter_ids = [int(id) for id in meter_ids if int(id) in dict]

                # # Create a tooltip dictionary with valid machine IDs and their kWh values
                # formula_tooltip = {meter_id: dict[meter_id] for meter_id in valid_formula_meter_ids}
                meter_ids = re.findall(r'dict\[(\d+)\]', formula)

                valid_formula_meter_ids = [int(id) for id in meter_ids if int(id) in dict]
                print(valid_formula_meter_ids)
                formula_tooltip = {meter_id_dict[meter_id]: dict[meter_id] for meter_id in valid_formula_meter_ids}
                print(formula_tooltip)
                datas.append({"group_name": group_name,"function_name": func_name,"function_value": results,"formula1": formula1,"tooltip":formula_tooltip})
        return datas
    except Exception as e:
        return get_exception_response(e)
    
def function_dashboard2(cnx):
    try:
        mill_date = date.today()
        mill_shift = 0
        group_name = ''
        func_name = ''
        formula1 = ''
        results = ''

        sql1 = f'select * from ems_v1.dbo.master_shifts'
        data = cnx.execute(sql1).mappings().all()
        if len(data)>0:
            for row in data:
                mill_date = row["mill_date"]
                mill_shift = row["mill_shift"]

        sql2 = f'SELECT * FROM [ems_v1].[dbo].[master_energy_calculations2] ORDER BY group_name, s_no'
        result = cnx.execute(sql2).mappings().all()
        if len(result)>0:
            para = ''
            for rows in result:
                para = rows['parameter']

            if para == 'kw':
                para = "case when mmf.kw = '*' then p.t_watts * mmf.kw_value when  mmf.kw = '/' then p.t_watts / mmf.kw_value else p.t_watts end "

            if para == 'kWh':
                para = "case when mmf.kWh = '*' then p.kWh * mmf.kWh_value when  mmf.kWh = '/' then p.kWh / mmf.kWh_value else p.kWh end "
        
            sql3 = text(f'''
                    select 
                        p.meter_id,
                        min(mm.machine_name) as machine_name,
                        sum({para}) as kWh 
                    from 
                        [ems_v1].[dbo].[current_power] p
                        left join  [ems_v1].[dbo].[master_machine] mm on mm.meter_id=p.meter_id
                        left join  [ems_v1].[dbo].[master_machine_factor] mmf on mm.meter_id=mmf.meter_id
                    where 
                        p.mill_date = '{mill_date}' and p.mill_shift = {mill_shift}
                    group by 
                        p.meter_id 
                    order by 
                        p.meter_id''')
            print(sql3)
            res = cnx.execute(sql3).mappings().all()

            meter_id_dict={}
            # dict_tt ={}
            dict={}
            for row in res:
                dict[row['meter_id']] = row['kWh']
                # dict_tt[row['machine_name']] = row['kWh']
                meter_id_dict[row['meter_id']] = row['machine_name']
            datas = []

            for rows in result:
                group_name = rows['group_name']
                func_name = rows['function_name']
                formula = rows['formula2']
                formula1 = rows['formula1']
                
                results = eval(formula, {"dict": dict})
                
                # meter_ids = re.findall(r'dict\[(\d+)\]', formula)
        
                # # Convert extracted IDs to integers and filter out IDs not present in the dict
                # valid_formula_meter_ids = [int(id) for id in meter_ids if int(id) in dict]

                # # Create a tooltip dictionary with valid machine IDs and their kWh values
                # formula_tooltip = {meter_id: dict[meter_id] for meter_id in valid_formula_meter_ids}
                meter_ids = re.findall(r'dict\[(\d+)\]', formula)

                valid_formula_meter_ids = [int(id) for id in meter_ids if int(id) in dict]
                print(valid_formula_meter_ids)
                formula_tooltip = {meter_id_dict[meter_id]: dict[meter_id] for meter_id in valid_formula_meter_ids}
                print(formula_tooltip)
                datas.append({"group_name": group_name,"function_name": func_name,"function_value": results,"formula1": formula1,"tooltip":formula_tooltip})
        return datas
    except Exception as e:
        return get_exception_response(e)
