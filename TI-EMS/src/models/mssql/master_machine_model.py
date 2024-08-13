from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from log_file import createFolder
def machine_Lists(cnx, company_id,machine_id,type_value,type_id,is_critical,model_name,department_id,shed_id,machinetype_id,function_id,function2_id,holiday):
    try:
        print("department_id",department_id) 
        if type_id !='':
            value = type_id.split(",")
            if len(value) > 1:
                values = tuple(value)
                type_id = ",".join(values)
            else:
                type_id = value[0]
                
        where = ""
        if company_id !='':
            where += f" and mm.company_id = {company_id}" 
        if machine_id !='' and machine_id != None:
            where += f" and mm.machine_id = {machine_id}" 
             
        if type_value !='' and type_id !='' and type_id != None:
            if type_value == 'zone':
                where += f" and mm.department_id in ({','.join(str(x) for x in value)})"
          
            elif type_value == 'area':
                where += f" and mm.shed_id in ({','.join(str(x) for x in value)})"

            elif type_value == 'location':
                where += f" and mm.machinetype_id in ({','.join(str(x) for x in value)})"

            elif type_value == 'function':
                where += f" and mm.function_id in ({','.join(str(x) for x in value)})"
                
        if is_critical=="yes" or is_critical=="no"  :
            where += f" and mm.major_nonmajor = '{is_critical}' "   
        
        if model_name !='':
            where += f" and mm.model_name" 
        
        if department_id !='' and department_id != 0 and department_id != None:
            where += f" and mm.department_id = {department_id}"
        
        if shed_id !='' and shed_id != 0 and shed_id != None:
            where +=f" and mm.shed_id = {shed_id}"
        
        if machinetype_id !='' and machinetype_id != 0 and machinetype_id != None:
            where += f" and mm.machinetype_id = {machinetype_id}"
            
        if function_id !='':
            where += f" and mm.function_id = {function_id}"  

        if function2_id !='':
            where += f" and mm.function2_id = {function2_id}" 

        if holiday !='':
            query1 = text(f'''
                        SELECT 
                            min(mhm.machine_id) as machine_id
                        FROM 
                            ems_v1.dbo.master_holiday mh
                        INNER JOIN ems_v1.dbo.master_holiday_machine mhm ON mh.id = mhm.ref_id
                        WHERE mh.status = 'active' and mh.holiday_year = '{holiday}'
                        GROUP BY mhm.machine_id 
                          ''') 
            data2 = cnx.execute(query1).mappings().all()
            createFolder("Log/","Issue in returning data "+str(data2))
            machine_id= []
            if len(data2)>0:
                for record in data2:
                    machine_id.append(record["machine_id"]) 
                        
                where += f" and mm.machine_id  not in ({','.join(str(x) for x in machine_id)})"
           
        query = text(f"""
                    SELECT 
                        mc.company_code AS company_code,
                        mc.company_name AS company_name,
                        mb.branch_code AS branch_code,
                        mb.branch_name AS branch_name,
                        md.department_code AS department_code,
                        md.department_name AS department_name,
                        ms.shed_code AS shed_code,
                        ms.shed_name AS shed_name,
                        mmt.machinetype_code AS machinetype_code,                        
                        mmt.machinetype_name AS machinetype_name,
                        mf.function_name  AS function1_name,
                        mf.function_code  AS function1_code,
                        mff.function_name  AS function2_name,
                        mff.function_code  AS function2_code,
                        mcd.converter_name AS converter_name,
                        mm.*,
                        ISNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	                    ISNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
                    FROM 
                        [ems_v1].[dbo].[master_machine] mm
                        left join [ems_v1].[dbo].[master_employee] cu on cu.employee_id=mm.created_by
	                    left join [ems_v1].[dbo].[master_employee] mu on mu.employee_id=mm.modified_by
                        INNER JOIN [ems_v1].[dbo].[master_company] mc ON mm.company_id = mc.company_id
                        INNER JOIN [ems_v1].[dbo].[master_branch] mb ON mm.branch_id = mb.branch_id
                        INNER JOIN [ems_v1].[dbo].[master_department] md ON mm.department_id = md.department_id
                        INNER JOIN [ems_v1].[dbo].[master_shed] ms ON mm.shed_id = ms.shed_id
                        INNER JOIN [ems_v1].[dbo].[master_machinetype] mmt ON mm.machinetype_id = mmt.machinetype_id
                        LEFT JOIN [ems_v1].[dbo].[master_function] mf ON mm.function_id = mf.function_id
                        LEFT JOIN [ems_v1].[dbo].[master_function] mff ON mm.function2_id = mff.function_id
                        INNER JOIN [ems_v1].[dbo].[master_converter_detail] mcd ON mm.converter_id = mcd.converter_id
                    WHERE 
                        mm.status != 'delete' {where} """)
    
        data = cnx.execute(query).mappings().all()
        return data
    except Exception as e:
        return get_exception_response(e)
    
def getmachinedtl(cnx, machine_id, machine_code, machine_name):
    where=""

    if machine_id != "":
        where += f"and machine_id <> '{machine_id}' "
      
    query=f'''select * from ems_v1.dbo.master_machine where 1=1 and status<>'delete' and machine_code='{machine_code}' {where}'''

    result = cnx.execute(text(query)).mappings().all()
    
    return result

def save_machine(cnx, company_id,branch_id,department_id,shed_id,machinetype_id,function_id,converter_id,machine_id,machine_code,machine_name,ip_address,port,major_nonmajor,model_name,energy_selection,energy_selection1,IMEI,user_login_id,function2_id):
    try: 
        query = text(f"""
                INSERT INTO [ems_v1].[dbo].[master_machine] (
                    company_id, machine_name, machine_code, branch_id, department_id, shed_id, converter_id, function_id,machinetype_id,
                    ip_address, port, created_on, created_by, major_nonmajor, model_name, energy_selection, IMEI, energy_selection1, function2_id
                )
                VALUES (
                    {company_id},'{machine_name}', '{machine_code}', {branch_id}, {department_id}, {shed_id}, {converter_id}, '{function_id}',
                    {machinetype_id}, '{ip_address}',{port}, GETDATE(), '{user_login_id}', '{major_nonmajor}', '{model_name}','{energy_selection}', {IMEI},'{energy_selection1}','{function2_id}'
                )
            """)
        cnx.execute(query)
        insert_id  = cnx.execute(text("SELECT SCOPE_IDENTITY()")).first()[0]
        cnx.commit()

        if insert_id !='': 
            query1 = text(f'''select * from ems_v1.dbo.master_machine_factor where machine_id = {insert_id}''')
            record = cnx.execute(query1).mappings().all() 
            if len(record) == 0:
                query2 = text(f'''insert into  ems_v1.dbo.master_machine_factor 
                (machine_id,machine_kWh ,machine_kWh_value,kWh,kWh_value,r_volt,r_volt_value,y_volt,y_volt_value,b_volt,b_volt_value,ry_volt,ry_volt_value,
                yb_volt,yb_volt_value,br_volt,br_volt_value,vll_avg,vll_avg_value,vln_avg,vln_avg_value,r_current,r_current_value,y_current,y_current_value,
                b_current,b_current_value,t_current,t_current_value,frequency,frequency_value,r_watts,r_watts_value,y_watts,y_watts_value,b_watts,b_watts_value,
                t_watts,t_watts_value,kw,kw_value,r_powerfactor,r_powerfactor_value,y_powerfactor,y_powerfactor_value,b_powerfactor,b_powerfactor_value,avg_powerfactor,
                avg_powerfactor_value,powerfactor,powerfactor_value,power_factor,power_factor_value,r_var,r_var_value,y_var,y_var_value,b_var,b_var_value,t_var,t_var_value,
                r_voltampere,r_voltampere_value,y_voltampere,y_voltampere_value,b_voltampere,b_voltampere_value,t_voltampere,t_voltampere_value,kvah,kvah_value,kvar,kvar_value,kva,kva_value,company_id, department_id, shed_id, machinetype_id)
                values({insert_id},'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,'*',1,{company_id},{department_id},{shed_id},{machinetype_id})

                ''')
                cnx.execute(query2)
                cnx.commit()
             
            sql = text(f''' select * from [ems_v1].[dbo].[current_power] where machine_id = {insert_id}''') 
            data = cnx.execute(sql).mappings().all()

            if len(data)==0:                       
                sql1 = text(f"select * from [ems_v1].[dbo].[master_machine] where machine_id = {insert_id}")

                data1 = cnx.execute(sql1).mappings().all()
                for row in data1:
                    machine_id = row["machine_id"]
                    company_id = row["company_id"]
                    branch_id = row["branch_id"]
                    department_id = row["department_id"]
                    shed_id = row["shed_id"]
                    machinetype_id = row["machinetype_id"]  

                sql2= text(f" select * from [ems_v1].[dbo].[master_shifts]  where company_id = {company_id} and branch_id = {branch_id} AND status = 'active' ")
                data2 = cnx.execute(sql2).mappings().all()
                
                
                if len(data2)>0:
                    for row in data2:
                        mill_date = row["mill_date"]
                        mill_shift = row["mill_shift"]  
                        
                    sql3 = text(f'''
                                INSERT INTO [ems_v1].[dbo].[current_power] (machine_id, date_time, date_time1,
                                mill_date, mill_shift,company_id, branch_id, department_id, shed_id, machinetype_id)
                                VALUES ({machine_id}, GETDATE(), GETDATE(), '{mill_date}', '{mill_shift}',{company_id},
                                {branch_id}, {department_id}, {shed_id}, {machinetype_id})
                                ''')  
                    cnx.execute(sql3)
                    cnx.commit()
                    createFolder("Log/"," current power" +str(sql3))
        
        return insert_id
    except Exception as e:
        return get_exception_response(e)
    
def update_machine(cnx, company_id,branch_id,department_id,shed_id,machinetype_id,function_id,converter_id,machine_id,machine_code,machine_name,ip_address,port,major_nonmajor,model_name,energy_selection,energy_selection1,IMEI,user_login_id,function2_id):
    try: 
        query =text(f"""
                UPDATE [ems_v1].[dbo].[master_machine]
                SET company_id = {company_id}, machinetype_id = {machinetype_id}, machine_code = '{machine_code}',machine_name = '{machine_name}',
                branch_id = {branch_id}, shed_id = {shed_id},converter_id = {converter_id}, department_id = {department_id},function_id = '{function_id}',
                ip_address = '{ip_address}', port = '{port}', modified_on = GETDATE(), modified_by = '{user_login_id}', 
                major_nonmajor = '{major_nonmajor}', model_name = '{model_name}', energy_selection = '{energy_selection}', IMEI = {IMEI},energy_selection1='{energy_selection1}',function2_id='{function2_id}'
                WHERE machine_id = '{machine_id}'
            """) 
        
        cnx.execute(query)
        cnx.commit()
        sql = text(f''' select * from [ems_v1].[dbo].[current_power] where machine_id = {machine_id}''') 
        data = cnx.execute(sql).mappings().all()
        if len(data)==0:                       
            sql1 = text(f"select * from [ems_v1].[dbo].[master_machine] where machine_id = {machine_id}")
            data1 = cnx.execute(sql1).mappings().all()
            for row in data1:
                machine_id = row["machine_id"]
                company_id = row["company_id"]
                branch_id = row["branch_id"]
                department_id = row["department_id"]
                shed_id = row["shed_id"]
                machinetype_id = row["machinetype_id"]  
            sql2= text(f" select * from [ems_v1].[dbo].[master_shifts]  where company_id = {company_id} and branch_id = {branch_id} AND status = 'active' ")
            data2 = cnx.execute(sql2).mappings().all()
            
            if len(data2)>0:
                for row in data2:
                    mill_date = row["mill_date"]
                    mill_shift = row["mill_shift"]  
                    
                sql3 = text(f'''
                            INSERT INTO [ems_v1].[dbo].[current_power] (machine_id, date_time, date_time1,
                            mill_date, mill_shift,company_id, branch_id, department_id, shed_id, machinetype_id)
                            VALUES ({machine_id}, GETDATE(), GETDATE(), '{mill_date}', '{mill_shift}',{company_id},
                            {branch_id}, {department_id}, {shed_id}, {machinetype_id})
                            ''')  
                cnx.execute(sql3)
                cnx.commit()
                createFolder("Log/"," current power" +str(sql3))
        
    except Exception as e:
        return get_exception_response(e)
    
def update_machineStatus(cnx, machine_id, status):
    if status != '':
        query=f''' Update ems_v1.dbo.master_machine Set status = '{status}' Where machine_id='{machine_id}' '''
    else: 
        query=f''' Update ems_v1.dbo.master_machine Set status = 'delete' Where machine_id='{machine_id}' '''
      
    cnx.execute(text(query))
    cnx.commit()
    


