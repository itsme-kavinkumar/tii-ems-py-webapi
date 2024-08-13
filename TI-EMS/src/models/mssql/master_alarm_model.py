from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from log_file import createFolder

def alarm_Lists(cnx, company_id,alarm_target_id,alarm_type,branch_id,department_id,shed_id,machinetype_id):
    try:
        where = ""
        if alarm_target_id != '':
            where += f"and at.alarm_target_id = '{alarm_target_id}' "
            
        if alarm_type != '':
            where += f" and at.alarm_type= '{alarm_type}' "

        if company_id !='' and company_id != "0":
            where += f" and at.company_id= '{company_id}' "

        if branch_id !='' and branch_id !="0":
            where += f" and at.branch_id= '{branch_id}' "

        if department_id !='' and department_id!= "0":
            where += f" and at.department_id= '{department_id}' "

        if shed_id !='' and shed_id != "0":
            where += f" and at.shed_id= '{shed_id}' "

        if machinetype_id !='' and machinetype_id !="0":
            where += f" and at.machinetype_id= '{machinetype_id}' "

        query=text(f''' 
                SELECT 
                    at.*, 
                    '' as machine_dtl,
                    mb.branch_code,
                    mb.branch_name,
                    md.department_code,
                    md.department_name,
                    ms.shed_code,
                    ms.shed_name,
                    mmt.machinetype_code,
                    mmt.machinetype_name,
                    ISNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	                ISNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
                FROM 
                    ems_v1.dbo.master_alarm_target at
                    left join [ems_v1].[dbo].[master_employee] cu on cu.employee_id=at.created_by
	                left join [ems_v1].[dbo].[master_employee] mu on mu.employee_id=at.modified_by
                    left join [ems_v1].[dbo].[master_company] mc on mc.company_id=at.company_id
                    left join [ems_v1].[dbo].[master_branch] mb on mb.branch_id=at.branch_id
                    left join [ems_v1].[dbo].[master_department] md on md.department_id=at.department_id
                    left join [ems_v1].[dbo].[master_shed] ms on ms.shed_id=at.shed_id
                    left join [ems_v1].[dbo].[master_machinetype] mmt on mmt.machinetype_id=at.machinetype_id 
                WHERE 
                    at.status <> 'delete'
                    {where} 
                ''')
        
        createFolder("Log/","Issue in returning data "+str(query))
        data = cnx.execute(query).mappings().all()
        result = []
        for row in data:
            machine_id_list = row["machine_id"].strip(",").split(",")     
            machine_dtl = ""
            for machine_id in machine_id_list:                             
                sub_query = text(f"SELECT * FROM ems_v1.dbo.master_machine WHERE machine_id = {machine_id}")
                sub_data = cnx.execute(sub_query).mappings().all()
                for sub_row in sub_data:
                    if machine_dtl != "":
                        machine_dtl += '\n' 
                    machine_dtl += f'''{sub_row['machine_name']}''' 
                    print(machine_dtl)           
            new_row = dict(row)
            new_row["machine_dtl"] = machine_dtl
            result.append(new_row)            
        
        return result
    except Exception as e:
        return get_exception_response(e)
    
def getalarmdtl(cnx, alarm_target_id,  alarm_name):
    where=""

    if alarm_target_id != "":
        where += f"and alarm_target_id <> '{alarm_target_id}' "
      
    query=f'''select * from ems_v1.dbo.master_alarm where 1=1 and status<>'delete' and alarm_name='{alarm_name}' {where}'''

    result = cnx.execute(text(query)).mappings().all()
    
    return result

def save_alarm(cnx,company_id,branch_id,department_id ,shed_id ,machinetype_id ,parameter_name,machine_id,alarm_name,alarm_type,alarm_duration,color_1,color_2,color_3,login_id):
    try: 
        where =''
        if company_id!='' and company_id!= 0 :
            where += f'where mm.company_id = {company_id}'
        
        if branch_id!='' and company_id!= 0:
            where += f'and mm.branch_id = {branch_id}'
        
        if department_id!='' and company_id!= 0:
            where += f'and mm.department_id = {department_id}'
        
        if shed_id!='' and company_id!= 0:
            where += f'and mm.shed_id = {shed_id}'
        
        if machinetype_id!='' and company_id!= 0:
            where += f'and mm.machinetype_id = {machinetype_id}'

        if machine_id == '':
            query = text(f'''
                        select 
                            DISTINCT mm.machine_id 
                        from 
                            ems_v1.dbo.master_machine mm
                        {where}          
            ''') 
            data=cnx.execute(query).mappings().all()
            machine_id = []  
            if len(data) > 0:
                for record in data:
                    machine_id.append(str(record["machine_id"]))  
            machine_id = ",".join(machine_id)  
        print(machine_id)
        if machine_id !='':
            value = machine_id.split(",")
            if len(value) > 1:
                values = ",".join(value)  
                machine_id = f",{values},"  
            else:
                machine_id = f",{value[0]},"

        query= text(f'''INSERT INTO ems_v1.dbo.master_alarm_target (machine_id,parameter_name,alarm_name,color_1,color_2,color_3,
                       created_on,created_by, alarm_duration, alarm_type ,company_id,department_id, shed_id, machinetype_id,branch_id)
                       VALUES ('{machine_id}','{parameter_name}','{alarm_name}','{color_1}', '{color_2}','{color_3}',
                       GETDATE(),'{login_id}', '{alarm_duration}', '{alarm_type}',{company_id},'{department_id}','{shed_id}','{machinetype_id}','{branch_id}') ''')
            
        cnx.execute(query)
        insert_id = cnx.execute(text("SELECT SCOPE_IDENTITY()")).first()[0]
        cnx.commit()

        return insert_id
    except Exception as e:
        return get_exception_response(e)
    
def update_alarm(cnx, alarm_target_id,company_id,branch_id,department_id ,shed_id ,machinetype_id ,parameter_name,machine_id,alarm_name,alarm_type,alarm_duration,color_1,color_2,color_3,login_id):
    try:   
        where =''
        if company_id!='' and company_id!= 0 :
            where += f'where mm.company_id = {company_id}'
        
        if branch_id!='' and company_id!= 0:
            where += f'and mm.branch_id = {branch_id}'
        
        if department_id!='' and company_id!= 0:
            where += f'and mm.department_id = {department_id}'
        
        if shed_id!='' and company_id!= 0:
            where += f'and mm.shed_id = {shed_id}'
        
        if machinetype_id!='' and company_id!= 0:
            where += f'and mm.machinetype_id = {machinetype_id}'

        if machine_id == '':
            query = text(f'''
                        select 
                            DISTINCT mm.machine_id 
                        from 
                            ems_v1.dbo.master_machine mm
                        {where}          
            ''') 
            data=cnx.execute(query).mappings().all()

            machine_id = []  
            if len(data) > 0:
                for record in data:
                    machine_id.append(str(record["machine_id"]))  
            machine_id = ",".join(machine_id)  
       
        if machine_id !='':
            value = machine_id.split(",")
            if len(value) > 1:
                values = ",".join(value)  
                machine_id = f",{values},"  
            else:
                machine_id = f",{value[0]},"

        query =text(f'''UPDATE  ems_v1.dbo.master_alarm_target SET machine_id='{machine_id}',parameter_name='{parameter_name}',
                       alarm_name='{alarm_name}',color_1='{color_1}',color_2='{color_2}',color_3='{color_3}',
                       modified_on = GETDATE(),modified_by='{login_id}', alarm_duration = {alarm_duration},alarm_type = '{alarm_type}',company_id = {company_id},
                       department_id = '{department_id}', shed_id = '{shed_id}', machinetype_id = '{machinetype_id}',branch_id='{branch_id}'
                       where alarm_target_id = '{alarm_target_id}'   
                       ''')
        
        cnx.execute(query)
        cnx.commit()
    except Exception as e:
        return get_exception_response(e)
    
def update_alarmStatus(cnx, alarm_target_id, status):
    if status != '':
        query=f''' Update ems_v1.dbo.master_alarm Set status = '{status}' Where alarm_target_id='{alarm_target_id}' '''
    else: 
        query=f''' Update ems_v1.dbo.master_alarm Set status = 'delete' Where alarm_target_id='{alarm_target_id}' '''
      
    cnx.execute(text(query))
    cnx.commit()

def alarm_popup_status(cnx,company_id):
    data=''
    query = text(f'''Update [ems_v1].[dbo].[master_company] set alarm_status = 0 where company_id = {company_id}''')
    cnx.execute(query)
    cnx.commit()
    return data
    
    