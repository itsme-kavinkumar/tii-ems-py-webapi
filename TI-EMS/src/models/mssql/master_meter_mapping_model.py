from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from src.models.image import save_image
import json
def meter_mapping_list(cnx, id,equipment_id):

    where = '' 
    if id !='':
        where = f" and mec.id = {id}"   

    if equipment_id !='':
        where = f" and mec.equipment_id = {equipment_id}"   
    
    query = text(f"""
        SELECT                
            mec.*,
            mc.company_code,
            mc.company_name,
            mb.bu_code,
            mb.bu_name,
            mp.plant_code,
            mp.plant_name,
            pd.plant_department_code,
            pd.plant_department_name,
            meg.equipment_group_code,
            meg.equipment_group_name,
            me.equipment_code,
            me.equipment_name,
            me.company_id,
            me.bu_id,
            me.plant_id,
            me.plant_department_id,
            me.equipment_group_id,
            ISNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	        ISNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
        from 
            ems_v1.dbo.master_equipment_calculations mec
            left join ems_v1.master_employee cu on cu.employee_id=mec.created_by
	        left join ems_v1.master_employee mu on mu.employee_id=mec.modified_by
            INNER JOIN ems_v1.master_equipment me ON me.equipment_id = mec.equipment_id
            INNER JOIN ems_v1.master_company mc ON mc.company_id = me.company_id
            INNER JOIN ems_v1.master_business_unit mb ON mb.bu_id = me.bu_id
            INNER JOIN ems_v1.master_plant mp ON mp.plant_id = me.plant_id
            INNER JOIN ems_v1.master_plant_wise_department pd ON pd.plant_department_id = me.plant_department_id
            Inner JOIN ems_v1.master_equipment_group meg ON meg.equipment_group_id = me.equipment_group_id 
        where mec.status != 'delete' {where}
    """)
    data = cnx.execute(query).mappings().all()    
      
    return data

def save_metermapping(cnx,equipment_id,parameter,meter,user_login_id):
    
    data = json.loads(meter)
    if len(data) > 0:
        for record in data:
           formula1 =  record["formula1"]
           formula2 =  record["formula2"]
           meter_ids =  record["meter_ids"]
           sql = f'''insert into master_equipment_calculations (equipment_id,formula1,formula2,parameter,created_on,created_by)
                    values('{equipment_id}','{formula1}','{formula2}','{parameter}',getdate(),{user_login_id})'''
           cnx.execute(sql)
           cnx.commit()
           meter_id_list = meter_ids.split(",")  
           for meter_id in meter_id_list: 
                query = text(f'''insert into master_equipment_meter(equipment_id,meter_id)
                                  values({equipment_id},{meter_id}) ''')
                cnx.execute(query)
                cnx.commit()
    
def update_metermapping(cnx,id,equipment_id,parameter,meter,user_login_id):

    data = json.loads(meter)
    if len(data) > 0:
        for record in data:
            formula1 =  record["formula1"]
            formula2 =  record["formula2"]
            meter_ids =  record["meter_ids"]

            sql1 = f" delete from master_equipment_meter where equipment_id = {equipment_id}"
            cnx.execute(sql1)

            sql2 = f'''update  master_equipment_calculations 
                    set 
                        equipment_id = {equipment_id},
                        formula1 = '{formula1}',
                        formula2 = '{formula2}',
                        modified_on = getdate(),
                        modified_by = {user_login_id} '''
            cnx.execute(sql2)
            cnx.commit()

            meter_id_list = meter_ids.split(",")  
            for meter_id in meter_id_list: 
                sql3 = text(f'''insert into master_equipment_meter(equipment_id,meter_id)
                                  values({equipment_id},{meter_id}) ''')
                cnx.execute(sql3)
                cnx.commit()
    
def update_metermappingStatus(cnx, id, status):
    
    if status != '':
        query=f''' Update ems_v1.master_equipment_calculations Set status = '{status}' Where id='{id}' '''
    else: 
        query=f''' Update ems_v1.master_equipment_calculations Set status = 'delete' Where id='{id}' '''
      
    cnx.execute(text(query))
    cnx.commit()
