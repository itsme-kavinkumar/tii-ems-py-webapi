from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from log_file import createFolder

def sld_dtl(cnx,sld_type):
    try:
        where = ''
        if sld_type == 1:
            where =f' where mm.machine_id in (6,7,8,11,12,13,16,17,18,19,20,21,22,23,24,25,26)'
        if sld_type == 2:
            where =f' where mm.machine_id in (27,28,30,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46)'
        if sld_type == 3:
            where =f' where mm.machine_id in (16,6,28,25,26,41,14)'
        sql = text(f'''
            select 
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
                min(cp.branch_id) as branch_id,
                min(cp.department_id) as department_id,
                min(cp.shed_id) as shed_id,
                min(cp.machinetype_id) as machinetype_id,
                min(mf.function_id) AS function_id,
                min(cp.machine_id) as machine_id,
                min(cp.design_id) as design_id,
                min(cp.beam_id) as beam_id,
                min(cp.date_time) as date_time,
                min(cp.date_time1) as date_time1,
                min(cp.mill_date) as mill_date,
                min(cp.mill_shift) as mill_shift,
                ROUND(SUM(case when mmf.vln_avg = '*' then cp.vln_avg * mmf.vln_avg_value when  mmf.vln_avg = '/' then cp.vln_avg / mmf.vln_avg_value else cp.vln_avg end ),2) AS vln_avg,
                ROUND(SUM(case when mmf.r_volt = '*' then cp.r_volt * mmf.r_volt_value when  mmf.r_volt = '/' then cp.r_volt / mmf.r_volt_value else cp.r_volt end ),2) AS r_volt,
                ROUND(SUM(case when mmf.y_volt = '*' then cp.y_volt * mmf.y_volt_value when  mmf.y_volt = '/' then cp.y_volt / mmf.y_volt_value else cp.y_volt end ),2) AS y_volt,
                ROUND(SUM(case when mmf.b_volt = '*' then cp.b_volt * mmf.b_volt_value when  mmf.b_volt = '/' then cp.b_volt / mmf.b_volt_value else cp.b_volt end ),2) AS b_volt,
                ROUND(SUM(case when mmf.vll_avg = '*' then cp.vll_avg * mmf.vll_avg_value when  mmf.vll_avg = '/' then cp.vll_avg / mmf.vll_avg_value else cp.vll_avg end ),2) AS vll_avg,
                ROUND(SUM(case when mmf.ry_volt = '*' then cp.ry_volt * mmf.ry_volt_value when  mmf.ry_volt = '/' then cp.ry_volt / mmf.ry_volt_value else cp.ry_volt end ),2) AS ry_volt,
                ROUND(SUM(case when mmf.yb_volt = '*' then cp.yb_volt * mmf.yb_volt_value when  mmf.yb_volt = '/' then cp.yb_volt / mmf.yb_volt_value else cp.yb_volt end ),2) AS yb_volt,
                ROUND(SUM(case when mmf.br_volt = '*' then cp.br_volt * mmf.br_volt_value when  mmf.br_volt = '/' then cp.br_volt / mmf.br_volt_value else cp.br_volt end ),2) AS br_volt,
                ROUND(SUM(case when mmf.br_volt = '*' then cp.br_volt * mmf.br_volt_value when  mmf.br_volt = '/' then cp.br_volt / mmf.br_volt_value else cp.br_volt end ),2) AS br_volt,
                ROUND(SUM(case when mmf.r_current = '*' then cp.r_current * mmf.r_current_value when  mmf.r_current = '/' then cp.r_current / mmf.r_current_value else cp.r_current end ),2) AS r_current,
                ROUND(SUM(case when mmf.y_current = '*' then cp.y_current * mmf.y_current_value when  mmf.y_current = '/' then cp.y_current / mmf.y_current_value else cp.y_current end ),2) AS y_current,
                ROUND(SUM(case when mmf.b_current = '*' then cp.b_current * mmf.b_current_value when  mmf.b_current = '/' then cp.b_current / mmf.b_current_value else cp.b_current end ),2) AS b_current,
                ROUND(SUM(case when mmf.t_current = '*' then cp.t_current * mmf.t_current_value when  mmf.t_current = '/' then cp.t_current / mmf.t_current_value else cp.t_current end ),2) AS t_current,
                ROUND(SUM(case when mmf.t_watts = '*' then cp.t_watts * mmf.t_watts_value when  mmf.t_watts = '/' then cp.t_watts / mmf.t_watts_value else cp.t_watts end ),2) AS t_watts,
                ROUND(SUM(case when mmf.r_watts = '*' then cp.r_watts * mmf.r_watts_value when  mmf.r_watts = '/' then cp.r_watts / mmf.r_watts_value else cp.r_watts end ),2) AS r_watts,
                ROUND(SUM(case when mmf.y_watts = '*' then cp.y_watts * mmf.y_watts_value when  mmf.y_watts = '/' then cp.y_watts / mmf.y_watts_value else cp.y_watts end ),2) AS y_watts,
                ROUND(SUM(case when mmf.b_watts = '*' then cp.b_watts * mmf.b_watts_value when  mmf.b_watts = '/' then cp.b_watts / mmf.b_watts_value else cp.b_watts end ),2) AS b_watts,
                ROUND(SUM(case when mmf.t_var = '*' then cp.t_var * mmf.t_var_value when  mmf.t_var = '/' then cp.t_var / mmf.t_var_value else cp.t_var end ),2) AS t_var,
                ROUND(SUM(case when mmf.r_var = '*' then cp.r_var * mmf.r_var_value when  mmf.r_var = '/' then cp.r_var / mmf.r_var_value else cp.r_var end ),2) AS r_var,
                ROUND(SUM(case when mmf.y_var = '*' then cp.y_var * mmf.y_var_value when  mmf.y_var = '/' then cp.y_var / mmf.y_var_value else cp.y_var end ),2) AS y_var,
                ROUND(SUM(case when mmf.b_var = '*' then cp.b_var * mmf.b_var_value when  mmf.b_var = '/' then cp.b_var / mmf.b_var_value else cp.b_var end ),2) AS b_var,
                ROUND(SUM(case when mmf.t_voltampere = '*' then cp.t_voltampere * mmf.t_voltampere_value when  mmf.t_voltampere = '/' then cp.t_voltampere / mmf.t_voltampere_value else cp.t_voltampere end ),2) AS t_voltampere,
                ROUND(SUM(case when mmf.r_voltampere = '*' then cp.r_voltampere * mmf.r_voltampere_value when  mmf.r_voltampere = '/' then cp.r_voltampere / mmf.r_voltampere_value else cp.r_voltampere end ),2) AS r_voltampere,
                ROUND(SUM(case when mmf.y_voltampere = '*' then cp.y_voltampere * mmf.y_voltampere_value when  mmf.y_voltampere = '/' then cp.y_voltampere / mmf.y_voltampere_value else cp.y_voltampere end ),2) AS y_voltampere,
                ROUND(SUM(case when mmf.b_voltampere = '*' then cp.b_voltampere * mmf.b_voltampere_value when  mmf.b_voltampere = '/' then cp.b_voltampere / mmf.b_voltampere_value else cp.b_voltampere end ),2) AS b_voltampere,
                ROUND(SUM(case when mmf.avg_powerfactor = '*' then cp.avg_powerfactor * mmf.avg_powerfactor_value when  mmf.avg_powerfactor = '/' then cp.avg_powerfactor / mmf.avg_powerfactor_value else cp.avg_powerfactor end ),2) AS avg_powerfactor,
                ROUND(SUM(case when mmf.r_powerfactor = '*' then cp.r_powerfactor * mmf.r_powerfactor_value when  mmf.r_powerfactor = '/' then cp.r_powerfactor / mmf.r_powerfactor_value else cp.r_powerfactor end ),2) AS r_powerfactor,
                ROUND(SUM(case when mmf.y_powerfactor = '*' then cp.y_powerfactor * mmf.y_powerfactor_value when  mmf.y_powerfactor = '/' then cp.y_powerfactor / mmf.y_powerfactor_value else cp.y_powerfactor end ),2) AS y_powerfactor,
                ROUND(SUM(case when mmf.b_powerfactor = '*' then cp.b_powerfactor * mmf.b_powerfactor_value when  mmf.b_powerfactor = '/' then cp.b_powerfactor / mmf.b_powerfactor_value else cp.b_powerfactor end ),2) AS b_powerfactor,
                ROUND(SUM(case when mmf.powerfactor = '*' then cp.powerfactor * mmf.powerfactor_value when  mmf.powerfactor = '/' then cp.powerfactor / mmf.powerfactor_value else cp.powerfactor end ),2) AS powerfactor,
                
                ROUND(SUM(case when mmf.kvah = '*' then cp.kvah * mmf.kvah_value when  mmf.kvah = '/' then cp.kvah / mmf.kvah_value else cp.kvah end ),2) AS kvah,
                ROUND(SUM(case when mmf.kw = '*' then cp.t_watts * mmf.kw_value when  mmf.kw = '/' then cp.t_watts / mmf.kw_value else cp.t_watts end ),2) AS kw,
                ROUND(SUM(case when mmf.kvar = '*' then cp.kvar * mmf.kvar_value when  mmf.kvar = '/' then cp.kvar / mmf.kvar_value else cp.kvar end ),2) AS kvar,
                ROUND(SUM(case when mmf.power_factor = '*' then cp.power_factor * mmf.power_factor_value when  mmf.power_factor = '/' then cp.power_factor / mmf.power_factor_value else cp.power_factor end ),2) AS power_factor,
                ROUND(SUM(case when mmf.kva = '*' then cp.kva * mmf.kva_value when  mmf.kva = '/' then cp.kva / mmf.kva_value else cp.kva end ),2) AS kva,
                ROUND(SUM(case when mmf.frequency = '*' then cp.frequency * mmf.frequency_value when  mmf.frequency = '/' then cp.frequency / mmf.frequency_value else cp.frequency end ),2) AS frequency,
                min(cp.machine_status) as machine_status,
                min(cp.status) as status,
                min(cp.created_on) as created_on,
                min(cp.created_by) as created_by,
                min(cp.modified_on) as modified_on,
                min(cp.modified_by) as modified_by,
                
                ROUND(SUM(case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end ),2) AS machine_kWh,
                ROUND(SUM(case when mmf.machine_kWh = '*' then cp.master_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.master_kwh / mmf.machine_kWh_value else cp.master_kwh end ),2) AS master_kwh,
                ROUND(SUM(case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value when  mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end ),2) AS kWh,
                 
                ROUND(SUM(case when mmf.machine_kWh = '*' then cp.reverse_machine_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.reverse_machine_kwh / mmf.machine_kWh_value else cp.reverse_machine_kwh end ),2) AS reverse_machine_kwh,
                ROUND(SUM(case when mmf.kWh = '*' then cp.reverse_master_kwh * mmf.kWh_value when  mmf.kWh = '/' then cp.reverse_master_kwh / mmf.kWh_value else cp.reverse_master_kwh end ),2) AS reverse_master_kwh,
                ROUND(SUM(case when mmf.kWh = '*' then cp.reverse_kwh * mmf.kWh_value when  mmf.kWh = '/' then cp.reverse_kwh / mmf.kWh_value else cp.reverse_kwh end ),2) AS reverse_kwh,
                
                min(mm.ip_address) as ip_address,
                min(mm.port) as port,
                CASE WHEN min(cp.date_time) <= DATEADD(minute, -2, getdate()) THEN 'S' ELSE 'N' END as nocom,       
                ROUND(SUM(CASE WHEN cp.mill_shift = 1 THEN case when mmf.kWh = '*' then cp.kwh * mmf.kwh_value when  mmf.kwh = '/' then cp.kwh / mmf.kwh_value else cp.kwh end ELSE 0 END),2) AS kwh_1,
                ROUND(SUM(CASE WHEN cp.mill_shift = 2 THEN case when mmf.kWh = '*' then cp.kwh * mmf.kwh_value when  mmf.kwh = '/' then cp.kwh / mmf.kwh_value else cp.kwh end ELSE 0 END),2) AS kwh_2,
                ROUND(SUM(CASE WHEN cp.mill_shift = 3 THEN case when mmf.kWh = '*' then cp.kwh * mmf.kwh_value when  mmf.kwh = '/' then cp.kwh / mmf.kwh_value else cp.kwh end ELSE 0 END),2) AS kwh_3,
                ROUND(SUM(CASE WHEN cp.mill_shift = 1 THEN case when mmf.machine_kwh = '*' then cp.master_kwh * mmf.machine_kwh_value when  mmf.machine_kwh = '/' then cp.master_kwh / mmf.machine_kwh_value else cp.master_kwh end ELSE 0 END),2) AS start_kwh_1,
                ROUND(SUM(CASE WHEN cp.mill_shift = 2 THEN case when mmf.machine_kwh = '*' then cp.master_kwh * mmf.machine_kwh_value when  mmf.machine_kwh = '/' then cp.master_kwh / mmf.machine_kwh_value else cp.master_kwh end ELSE 0 END),2) AS start_kwh_2,
                ROUND(SUM(CASE WHEN cp.mill_shift = 3 THEN case when mmf.machine_kwh = '*' then cp.master_kwh * mmf.machine_kwh_value when  mmf.machine_kwh = '/' then cp.master_kwh / mmf.machine_kwh_value else cp.master_kwh end ELSE 0 END),2) AS start_kwh_3,     
                ROUND(SUM(CASE WHEN cp.mill_shift = 1 THEN case when mmf.machine_kwh = '*' then cp.machine_kwh * mmf.machine_kwh_value when  mmf.machine_kwh = '/' then cp.machine_kwh / mmf.machine_kwh_value else cp.machine_kwh end ELSE 0 END),2) AS end_kwh_1,
                ROUND(SUM(CASE WHEN cp.mill_shift = 2 THEN case when mmf.machine_kwh = '*' then cp.machine_kwh * mmf.machine_kwh_value when  mmf.machine_kwh = '/' then cp.machine_kwh / mmf.machine_kwh_value else cp.machine_kwh end ELSE 0 END),2) AS end_kwh_2,
                ROUND(SUM(CASE WHEN cp.mill_shift = 3 THEN case when mmf.machine_kwh = '*' then cp.machine_kwh * mmf.machine_kwh_value when  mmf.machine_kwh = '/' then cp.machine_kwh / mmf.machine_kwh_value else cp.machine_kwh end ELSE 0 END),2) AS end_kwh_3                    
            from
                ems_v1.dbo.current_power cp
                INNER JOIN [ems_v1].[dbo].[master_machine] mm ON cp.machine_id = mm.machine_id
                INNER JOIN [ems_v1].[dbo].[master_company] mc ON mm.company_id = mc.company_id
                INNER JOIN [ems_v1].[dbo].[master_branch] mb ON mm.branch_id = mb.branch_id
                INNER JOIN [ems_v1].[dbo].[master_department] md ON mm.department_id = md.department_id
                INNER JOIN [ems_v1].[dbo].[master_shed] ms ON mm.shed_id = ms.shed_id
                INNER JOIN [ems_v1].[dbo].[master_machinetype] mmt ON mm.machinetype_id = mmt.machinetype_id 
                LEFT JOIN [ems_v1].[dbo].[master_function] mf ON mm.function_id = mf.function_id
                LEFT JOIN [ems_v1].[dbo].[master_function] mff ON mm.function2_id = mff.function_id
                LEFT JOIN [ems_v1].[dbo].[master_machine_factor] mmf ON mm.machine_id = mmf.machine_id
            {where} 
            group by cp.machine_id
            ''')
        print(sql)
        data = cnx.execute(sql).mappings().all()
                
        return data
    except Exception as e:
        return get_exception_response(e)
    
