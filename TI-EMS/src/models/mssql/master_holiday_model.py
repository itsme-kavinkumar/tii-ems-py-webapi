from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from log_file import createFolder

def holidaylist(cnx, id,holiday_type):
    try:
        data2 = ''
        where = ''
        if id != '':
            where +=f'and mh.id = {id}'

        query = text(f'''
                    select 
                        mh.id ,
                        mh.holiday_year,
                        '' as machine_dtl,
                        GROUP_CONCAT(mhm.meter_id SEPARATOR ',') AS meter_id,
                        IFNULL(mh.status,'') status,
                        IFNULL(mhm.plant_id,'') plant_id,
                        IFNULL(mhm.plant_department_id,'') plant_department_id,
                        IFNULL(mhm.equipment_group_id,'') equipment_group_id,
                        IFNULL(md.department_code,'') department_code,
                        IFNULL(md.department_name,'') department_name,
                        IFNULL(ms.shed_code,'') shed_code,
                        IFNULL(ms.shed_name,'') shed_name,
                        IFNULL(mmt.machinetype_code,'') machinetype_code,
                        IFNULL(mmt.machinetype_name,'') machinetype_name,
                        IfNULL(concat(min(cu.employee_code),'-',min(cu.employee_name)),'') as created_user,
	                    IfNULL(concat(min(mu.employee_code),'-',min(mu.employee_name)),'') as modified_user,
                        mh.created_on ,
                        mh.modified_on 
                    from ems_v1.dbo.master_holiday mh
                    inner join ems_v1.master_holiday_machine mhm on mh.id = mhm.ref_id
                    left join ems_v1.master_plant md on mhm.plant_id = md.plant_id
                    left join ems_v1.master_plant_wise_department ms on mhm.plant_department_id = ms.plant_department_id
                    left join ems_v1.master_equipment_group mmt on mhm.equipment_group_id = mmt.equipment_group_id
                    left join ems_v1.master_employee cu on cu.employee_id=mh.created_by
	                left join ems_v1.master_employee mu on mu.employee_id=mh.modified_by 
                    where mh.status != 'delete' {where}
                    group by mh.id 
                     
                    ''')
        print(query)
        data = cnx.execute(query).fetchall()
        result = []
          
        for row in data:
            meter_id = row["meter_id"] 
            meter_id_list = meter_id.split(",")   
            machine_dtl = ""
            for meter_id in meter_id_list:                             
                sub_query = text(f"SELECT * FROM ems_v1.dbo.master_machine WHERE meter_id = {meter_id}")
                sub_data = cnx.execute(sub_query).fetchall()
                for sub_row in sub_data:
                    if machine_dtl != "":
                        machine_dtl += '\n' 
                    machine_dtl += f'''{sub_row['machine_name']}''' 
                    print(machine_dtl)           
            new_row = dict(row)
            new_row["machine_dtl"] = machine_dtl
            result.append(new_row)

        where_d = ''
        if id != '':
            if holiday_type != '':
                where_d += f" and holiday_type = '{holiday_type}'"
            query2 = text(f''' 
                        select 
                            id,
                            ref_id,
                            DATE_FORMAT(holiday_date,'%d-%m-%Y') as holiday_date,
                            description,
                            holiday_type,
                            Weekend_day
                        from 
                            ems_v1.dbo.master_holiday_date 
                        where ref_id = {id} {where_d}''')
            data2 = cnx.execute(query2).fetchall()
                           
        return {"data":result,"data2":data2}
    except Exception as e:
        return get_exception_response(e)
    
def save_holiday_dtl(cnx, year ,meter_id ,plant_id ,plant_department_id ,equipment_group_id ,obj  ,obj2,user_login_id):
    try: 
        if plant_id == None:
            plant_id = ''
        if plant_department_id == None:
            plant_department_id = ''
        if equipment_group_id == None:
            equipment_group_id = ''

        
        query = text(f''' insert into ems_v1.dbo.master_holiday (holiday_year,created_on,created_by)
                         values({year}, getdate(), {user_login_id})''')
        createFolder("Holiday_Log/", "master_holiday" + str(query)) 
        cnx.execute(query)
        insert_id = cnx.execute(text("SELECT LAST_INSERT_ID()")).first()[0]
        cnx.commit()
        
        meter_id_list = meter_id.split(",")  
        for meter_id in meter_id_list: 
            query2 = text(f''' insert into ems_v1.dbo.master_holiday_machine (ref_id, meter_id, plant_id, plant_department_id, equipment_group_id)
                        values ({insert_id},'{meter_id}','{plant_id}','{plant_department_id}','{equipment_group_id}')''')
            createFolder("Holiday_Log/", "master_holiday_machine" + str(query2)) 
            cnx.execute(query2)
            cnx.commit()
        obj_data = json.loads(obj)
        if obj !="":
            for row in obj_data:
                holiday_date = row["holiday_date"]
                description = row["description"]
                holiday_type = row["holiday_type"]
                holiday_date = '-'.join(reversed(holiday_date.split('-')))
                print(holiday_date)
                query3 = text(f'''INSERT INTO ems_v1.dbo.master_holiday_date 
                                (ref_id,holiday_date,description,holiday_type)
                                values({insert_id},'{holiday_date}','{description}','{holiday_type}')''')
                createFolder("Holiday_Log/", "master_holiday_date" + str(query3)) 
                cnx.execute(query3)
                cnx.commit()
        if obj2 !="":
            obj_data2 = json.loads(obj2)
            for row in obj_data2:
                holiday_date = row["holiday_date"]
                print("holiday_date",holiday_date)
                Weekend_day = row["Weekend_day"]
                holiday_type = row["holiday_type"]
                holiday_date = '-'.join(reversed(holiday_date.split('-')))
                # print(holiday_date)
                query3 = text(f'''INSERT INTO ems_v1.dbo.master_holiday_date 
                                (ref_id,holiday_date,holiday_type,Weekend_day)
                                values({insert_id},'{holiday_date}','{holiday_type}','{Weekend_day}')''')
                cnx.execute(query3)
                cnx.commit()

        return insert_id
    except Exception as e:
        return get_exception_response(e)
    
def upadte_holiday_dtl(cnx, year ,meter_id ,plant_id ,plant_department_id ,equipment_group_id ,obj, obj2,id ,user_login_id):
    try: 
        if plant_id == None:
            plant_id = ''
        if plant_department_id == None:
            plant_department_id = ''
        if equipment_group_id == None:
            equipment_group_id = ''

        del_query1=text(f'''DELETE FROM ems_v1.dbo.master_holiday_machine  where ref_id = '{id}' ''')
        del_query2=text(f'''DELETE FROM ems_v1.dbo.master_holiday_date  where ref_id = '{id}' ''')
        cnx.execute(del_query1)
        cnx.execute(del_query2)
        cnx.commit()
        query = text(f''' update ems_v1.dbo.master_holiday
                            set
                            holiday_year = '{year}',
                            modified_on = getdate(),
                            modified_by = '{user_login_id}' 
                            where id = {id}''')
        createFolder("Holiday_Log/", "master_holiday" + str(query)) 
        cnx.execute(query)
        cnx.commit()
        meter_id_list = meter_id.split(",")  
        for meter_id in meter_id_list: 
            query2 = text(f''' insert into ems_v1.dbo.master_holiday_machine (ref_id, meter_id, plant_id, plant_department_id, equipment_group_id)
                        values ({id},'{meter_id}','{plant_id}','{plant_department_id}','{equipment_group_id}')''')
            createFolder("Holiday_Log/", "master_holiday_machine" + str(query2)) 
            cnx.execute(query2)
            cnx.commit()
        obj_data = json.loads(obj)
        if obj !="":
            for row in obj_data:
                holiday_date = row["holiday_date"]
                description = row["description"]
                holiday_type = row["holiday_type"]
                holiday_date = '-'.join(reversed(holiday_date.split('-')))
                print("holiday_date",holiday_date)
                query3 = text(f'''
                            insert into  ems_v1.dbo.master_holiday_date
                            (holiday_date,description,holiday_type,ref_id)
                            values('{holiday_date}','{description}','{holiday_type}','{id}')
                                 ''')
                createFolder("Holiday_Log/", "master_holiday_date" + str(query3)) 
                cnx.execute(query3)
                cnx.commit()
        if obj2 !="":
            obj_data2 = json.loads(obj2)
            for row in obj_data2:
                holiday_date = row["holiday_date"]
                holiday_type = row["holiday_type"]
                Weekend_day = row["Weekend_day"]
                holiday_date = '-'.join(reversed(holiday_date.split('-')))
                print("holiday_date",holiday_date)
                query3 = text(f'''
                            insert into  ems_v1.dbo.master_holiday_date
                            (holiday_date,holiday_type,ref_id,Weekend_day)
                            values('{holiday_date}','{holiday_type}','{id}','{Weekend_day}')
                                 ''')
                cnx.execute(query3)
                cnx.commit()

    except Exception as e:
        return get_exception_response(e)

def upadte_holidaystatus(cnx, id, status):

    if status !='':
        query = text(f" UPDATE [ems_v1].[dbo].[master_holiday] SET status = '{status}' WHERE id = '{id}' ")
        cnx.execute(query)
    else:
        query = text(f" UPDATE [ems_v1].[dbo].[master_holiday] SET status = 'delete' WHERE id = '{id}' ")                
        cnx.execute(query)

        del_query1=text(f'''DELETE FROM [ems_v1].[dbo].[master_holiday_machine]  where ref_id = '{id}' ''')
        del_query2=text(f'''DELETE FROM [ems_v1].[dbo].[master_holiday_date]  where ref_id = '{id}' ''')
        cnx.execute(del_query1)
        cnx.execute(del_query2)

    cnx.commit()
    createFolder("Holiday_Log/","query execute sucessfully")
