from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from src.models.image import parse_date
from datetime import date
def load_analysis_mdl(cnx,period_id,machine_id,from_date,to_date,shift_id,from_time,to_time,duration):
    try:
        mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}    
    
        where = ''
        table_name = ''
        if duration == "":
            duration = 1
        
        if period_id == 'cur_shift': 
            query=text(f'''SELECT * FROM [ems_v1].[dbo].[master_shifts] WHERE status='active' ''')
            data1 = cnx.execute(query).mappings().all()
            mill_date = date.today()
            mill_shift = 0       
            if len(data1) > 0:
                for shift_record in data1:
                    mill_date = shift_record["mill_date"]
                    mill_shift = shift_record["mill_shift"]  
                        
            table_name = 'ems_v1.dbo.current_power_analysis cp'
            where += f"cp.mill_date = '{mill_date}' and cp.mill_shift ='{mill_shift}' "

        elif period_id == 'sel_shift' or period_id == 'sel_date':
            if from_date == '':
                return _getErrorResponseJson("date is required") 
            
            mill_date=parse_date(from_date)             
            month_year=f"""{mill_month[mill_date.month]}{str(mill_date.year)}"""
            table_name=f"[ems_v1_completed].[dbo].[power_analysis_{month_year}]" 
            where += f"cp.mill_date = '{mill_date}' "

            field_name = 'id,machine_id, created_on, mill_date, mill_shift, t_current, r_current, y_current, b_current, vll_avg, ry_volt, yb_volt, br_volt, vln_avg, r_volt, y_volt, b_volt, t_watts, kWh, kvah, kw, kvar, power_factor, r_watts, kva, y_watts, b_watts, avg_powerfactor, r_powerfactor, y_powerfactor, b_powerfactor, powerfactor, kwh_actual, frequency, t_voltampere, r_voltampere, y_voltampere, b_voltampere, t_var, r_var, y_var, b_var, master_kwh, machine_kwh'
            table_name = f'(select {field_name} from [ems_v1].[dbo].[current_power_analysis] UNION All select {field_name} from {table_name})cp'

            if period_id == 'sel_shift':
                if shift_id == '':
                    return _getErrorResponseJson("shift is required") 
                where += f" and cp.mill_shift ='{shift_id}' " 
            if month_year !='':
                query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'ems_v1_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'power_analysis_{month_year}'"""
                print(query)
                result_query = cnx.execute(query).mappings().all()
                if len(result_query)>0:
                    pass
                else:
                    return _getErrorResponseJson("analysis table not available...")    
   
        elif period_id == "from_to":            
            if from_date == '':
                return _getErrorResponseJson("from date is required")
            if to_date == '':
                 return _getErrorResponseJson("to_date is required")  
                    
            from_date = parse_date(from_date)
            to_date =  parse_date(to_date)
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""       
        
            where += f'''  cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            
            if shift_id != "":                
                where += f''' and cp.mill_shift = '{shift_id}' ''' 
            field_name = 'id,machine_id, created_on, mill_date, mill_shift, t_current, r_current, y_current, b_current, vll_avg, ry_volt, yb_volt, br_volt, vln_avg, r_volt, y_volt, b_volt, t_watts, kWh, kvah, kw, kvar, power_factor, r_watts, kva, y_watts, b_watts, avg_powerfactor, r_powerfactor, y_powerfactor, b_powerfactor, powerfactor, kwh_actual, frequency, t_voltampere, r_voltampere, y_voltampere, b_voltampere, t_var, r_var, y_var, b_var, master_kwh, machine_kwh'
            
            if from_date.month == to_date.month:
                query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'ems_v1_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'power_analysis_{month_year}'"""
                result_query = cnx.execute(query).mappings().all()
                print(query)
                if len(result_query) == 0:
                    return _getErrorResponseJson("analysis table not available...")    
       
                table_name=f"[ems_v1_completed].[dbo].[power_analysis_{month_year}]" 
                table_name = f'(select {field_name} from [ems_v1].[dbo].[current_power_analysis] UNION All select {field_name} from {table_name})cp'
            else:
                from_month = from_date.month
                to_month = to_date.month
                month_year_range = [
                f"{mill_month[month]}{str(from_date.year)}" for month in range(from_month, to_month + 1)
                ]
                union_queries = []

                for month_year in month_year_range:
                    query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'ems_v1_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'power_analysis_{month_year}'"""
                    result_query = cnx.execute(query).mappings().all()
                    print(query)
                    if len(result_query) > 0:
                        table_name = f"[ems_v1_completed].[dbo].[power_analysis_{month_year}]"
                        union_queries.append(f"SELECT {field_name} FROM {table_name}")

                    if len(union_queries) == 0:
                        return _getErrorResponseJson("analysis table not available...")    

                subquery_union = " UNION ALL ".join(union_queries)
                table_name = f"(SELECT {field_name} FROM [ems_v1].[dbo].[current_power_analysis] UNION ALL {subquery_union}) cp"

        if from_time !='':
            where += f" and FORMAT(cp.created_on ,'HH:mm:ss')>='{from_time}' "
    
        if to_time !='':
            where += f" and FORMAT(cp.created_on ,'HH:mm:ss')<='{to_time}' "
                
        query=text(f'''
            SELECT *
            FROM (
                SELECT 
                (ROW_NUMBER() OVER (ORDER BY cp.id) - 1) % {duration} + 1 AS slno,
			    mm.machine_code,
			    mm.machine_name,
			    cp.machine_id,
                FORMAT(cp.created_on, 'yyyy-MM-ddTHH:mm:ss') as date_time,
			    cp.mill_date,
			    cp.mill_shift,
			    ROUND((case when mmf.t_current = '*' then cp.t_current * mmf.t_current_value  when mmf.t_current = '/' then cp.t_current / mmf.t_current_value else cp.t_current end),2) AS t_current,
                ROUND((case when mmf.r_current = '*' then cp.r_current * mmf.r_current_value  when mmf.r_current = '/' then cp.r_current / mmf.r_current_value else cp.r_current end),2) AS r_current,
			    ROUND((case when mmf.y_current = '*' then cp.y_current * mmf.y_current_value  when mmf.y_current = '/' then cp.y_current / mmf.y_current_value else cp.y_current end),2) AS y_current,
			    ROUND((case when mmf.b_current = '*' then cp.b_current * mmf.b_current_value  when mmf.b_current = '/' then cp.b_current / mmf.b_current_value else cp.b_current end),2) AS b_current,
			    ROUND((case when mmf.vll_avg = '*' then cp.vll_avg * mmf.vll_avg_value  when mmf.vll_avg = '/' then cp.vll_avg / mmf.vll_avg_value else cp.vll_avg end),2) AS vll_avg,
			    ROUND((case when mmf.ry_volt = '*' then cp.ry_volt * mmf.ry_volt_value  when mmf.ry_volt = '/' then cp.ry_volt / mmf.ry_volt_value else cp.ry_volt end),2) AS ry_volt,
			    ROUND((case when mmf.yb_volt = '*' then cp.yb_volt * mmf.yb_volt_value  when mmf.yb_volt = '/' then cp.yb_volt / mmf.yb_volt_value else cp.yb_volt end),2) AS yb_volt,
			    ROUND((case when mmf.br_volt = '*' then cp.br_volt * mmf.br_volt_value  when mmf.br_volt = '/' then cp.br_volt / mmf.br_volt_value else cp.br_volt end),2) AS br_volt,
			    ROUND((case when mmf.vln_avg = '*' then cp.vln_avg * mmf.vln_avg_value  when mmf.vln_avg = '/' then cp.vln_avg / mmf.vln_avg_value else cp.vln_avg end),2) AS vln_avg,
			    ROUND((case when mmf.r_volt = '*' then cp.r_volt * mmf.r_volt_value  when mmf.r_volt = '/' then cp.r_volt / mmf.r_volt_value else cp.r_volt end),2) AS r_volt,
			    ROUND((case when mmf.y_volt = '*' then cp.y_volt * mmf.y_volt_value  when mmf.y_volt = '/' then cp.y_volt / mmf.y_volt_value else cp.y_volt end),2) AS y_volt,
			    ROUND((case when mmf.b_volt = '*' then cp.b_volt * mmf.b_volt_value  when mmf.b_volt = '/' then cp.b_volt / mmf.b_volt_value else cp.b_volt end),2) AS b_volt,
			    ROUND((case when mmf.t_watts = '*' then cp.t_watts * mmf.t_watts_value  when mmf.t_watts = '/' then cp.t_watts / mmf.t_watts_value else cp.t_watts end),2) AS t_watts,
                ROUND((case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value  when mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end),2) AS kWh,
			    ROUND((case when mmf.kvah = '*' then cp.kvah * mmf.kvah_value  when mmf.kvah = '/' then cp.kvah / mmf.kvah_value else cp.kvah end),2) AS kvah,
			    ROUND((case when mmf.kw = '*' then cp.t_watts * mmf.kw_value  when mmf.kw = '/' then cp.t_watts / mmf.kw_value else cp.t_watts end),2)  AS kw,
			    ROUND((case when mmf.kvar = '*' then cp.kvar * mmf.kvar_value  when mmf.kvar = '/' then cp.kvar / mmf.kvar_value else cp.kvar end),2) AS kvar,
			    ROUND((case when mmf.power_factor = '*' then cp.power_factor * mmf.power_factor_value  when mmf.power_factor = '/' then cp.power_factor / mmf.power_factor_value else cp.power_factor end),2) AS power_factor,
			    ROUND((case when mmf.r_watts = '*' then cp.r_watts * mmf.r_watts_value  when mmf.r_watts = '/' then cp.r_watts / mmf.r_watts_value else cp.r_watts end),2) AS r_watts,
			    ROUND((case when mmf.kva = '*' then cp.kva * mmf.kva_value  when mmf.kva = '/' then cp.kva / mmf.kva_value else cp.kva end),2) AS kva,
			    ROUND((case when mmf.y_watts = '*' then cp.y_watts * mmf.y_watts_value  when mmf.y_watts = '/' then cp.y_watts / mmf.y_watts_value else cp.y_watts end),2) AS y_watts,
			    ROUND((case when mmf.b_watts = '*' then cp.b_watts * mmf.b_watts_value  when mmf.b_watts = '/' then cp.b_watts / mmf.b_watts_value else cp.b_watts end),2) AS b_watts,
			    ROUND((case when mmf.avg_powerfactor = '*' then cp.avg_powerfactor * mmf.avg_powerfactor_value  when mmf.avg_powerfactor = '/' then cp.avg_powerfactor / mmf.avg_powerfactor_value else cp.avg_powerfactor end),2) AS avg_powerfactor,
			    ROUND((case when mmf.r_powerfactor = '*' then cp.r_powerfactor * mmf.r_powerfactor_value  when mmf.r_powerfactor = '/' then cp.r_powerfactor / mmf.r_powerfactor_value else cp.r_powerfactor end),2) AS r_powerfactor,
			    ROUND((case when mmf.y_powerfactor = '*' then cp.y_powerfactor * mmf.y_powerfactor_value  when mmf.y_powerfactor = '/' then cp.y_powerfactor / mmf.y_powerfactor_value else cp.y_powerfactor end),2) AS y_powerfactor,
			    ROUND((case when mmf.b_powerfactor = '*' then cp.b_powerfactor * mmf.b_powerfactor_value  when mmf.b_powerfactor = '/' then cp.b_powerfactor / mmf.b_powerfactor_value else cp.b_powerfactor end),2) AS b_powerfactor,
			    ROUND((case when mmf.powerfactor = '*' then cp.powerfactor * mmf.powerfactor_value  when mmf.powerfactor = '/' then cp.powerfactor / mmf.powerfactor_value else cp.powerfactor end),2) AS powerfactor,
			    ROUND((case when mmf.kWh = '*' then cp.kwh_actual * mmf.kWh_value  when mmf.kWh = '/' then cp.kwh_actual / mmf.kWh_value else cp.kwh_actual end),2) AS kwh_actual,
			    ROUND((case when mmf.frequency = '*' then cp.frequency * mmf.frequency_value  when mmf.frequency = '/' then cp.frequency / mmf.frequency_value else cp.frequency end),2) AS frequency,
			    ROUND((case when mmf.t_voltampere = '*' then cp.t_voltampere * mmf.t_voltampere_value  when mmf.t_voltampere = '/' then cp.t_voltampere / mmf.t_voltampere_value else cp.t_voltampere end),2) AS t_voltampere,
			    ROUND((case when mmf.r_voltampere = '*' then cp.r_voltampere * mmf.r_voltampere_value  when mmf.r_voltampere = '/' then cp.r_voltampere / mmf.r_voltampere_value else cp.r_voltampere end),2) AS r_voltampere,
			    ROUND((case when mmf.y_voltampere = '*' then cp.y_voltampere * mmf.y_voltampere_value  when mmf.y_voltampere = '/' then cp.y_voltampere / mmf.y_voltampere_value else cp.y_voltampere end),2) AS y_voltampere,
			    ROUND((case when mmf.b_voltampere = '*' then cp.b_voltampere * mmf.b_voltampere_value  when mmf.b_voltampere = '/' then cp.b_voltampere / mmf.b_voltampere_value else cp.b_voltampere end),2) AS b_voltampere,
			    ROUND((case when mmf.t_var = '*' then cp.t_var * mmf.t_var_value  when mmf.t_var = '/' then cp.t_var / mmf.t_var_value else cp.t_var end),2) AS t_var,
			    ROUND((case when mmf.r_var = '*' then cp.r_var * mmf.r_var_value  when mmf.r_var = '/' then cp.r_var / mmf.r_var_value else cp.r_var end),2) AS r_var,
			    ROUND((case when mmf.y_var = '*' then cp.y_var * mmf.y_var_value  when mmf.y_var = '/' then cp.y_var / mmf.y_var_value else cp.y_var end),2) AS y_var,
			    ROUND((case when mmf.b_var = '*' then cp.b_var * mmf.b_var_value  when mmf.b_var = '/' then cp.b_var / mmf.b_var_value else cp.b_var end),2) AS b_var,
                ROUND((case when mmf.machine_kWh = '*' then cp.master_kwh * mmf.machine_kWh_value  when mmf.machine_kWh = '/' then cp.master_kwh / mmf.machine_kWh_value else cp.master_kwh end),2) AS master_kwh,
                ROUND((case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value  when mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end),2) AS machine_kWh
		    from
                {table_name}   

		        inner join [ems_v1].[dbo].[master_machine] mm on mm.machine_id=cp.machine_id
                left join [ems_v1].[dbo].[master_machine_factor] mmf on mmf.machine_id=mm.machine_id
                
		    where 
                cp.machine_id in ({machine_id}) and {where} ) AS subquery
            WHERE
                slno = 1
		    order by machine_id, date_time                                
            ''')  

        data=cnx.execute(query).mappings().all()
        label = {}
        machine_data = {}
        org_data = []
        for d in data:
            machine_id = d['machine_id']
            machine_name = d['machine_name']
            if machine_id not in label:        
                label[machine_id] = machine_name
            if machine_id not in machine_data:
                machine_data[machine_id] = []

            # set machine_data for machine_id
            temp = {
                'date_time': d['date_time'],
                't_current': d['t_current'],
                'r_current': d['r_current'],
                'y_current': d['y_current'],
                'b_current': d['b_current'],
                'vll_avg': d['vll_avg'],
                'ry_volt': d['ry_volt'],
                'yb_volt': d['yb_volt'],
                'br_volt': d['br_volt'],
                'vln_avg': d['vln_avg'],
                'r_volt': d['r_volt'],
                'y_volt': d['y_volt'],
                'b_volt': d['b_volt'],
                't_watts': d['t_watts'],
                'kWh': d['kWh'],
                'kvah': d['kvah'],
                'kw': d['kw'],
                'kvar': d['kvar'],
                'power_factor': d['power_factor'],
                'r_watts': d['r_watts'],
                'kva': d['kva'],
                'y_watts': d['y_watts'],
                'b_watts': d['b_watts'],
                'avg_powerfactor': d['avg_powerfactor'],
                'r_powerfactor': d['r_powerfactor'],
                'y_powerfactor': d['y_powerfactor'],
                'b_powerfactor': d['b_powerfactor'],
                'powerfactor': d['powerfactor'],
                'kwh_actual': d['kwh_actual'],
                'frequency': d['frequency'],
                't_voltampere': d['t_voltampere'],
                'r_voltampere': d['r_voltampere'],
                'y_voltampere': d['y_voltampere'],
                'b_voltampere': d['b_voltampere'],
                't_var': d['t_var'],
                'r_var': d['r_var'],
                'y_var': d['y_var'],
                'b_var': d['b_var'],
                'master_kwh':d['master_kwh'],
                'machine_kWh':d['machine_kWh']
            }

            machine_data[machine_id].append(temp)

        for key, value in machine_data.items():
            org_data.append({'label': label[key], 'data': value})

        return {"data":org_data,"data1":data}
    except Exception as e:
        return get_exception_response(e)