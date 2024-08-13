# from src.models.mysql.plant_wise_sync_and_is_assign import update_plant_wise_sync
from sqlalchemy import text
from src.models.mysql.master_plant_model import plant_Lists
from src.models.mysql.master_equipment_model import equipment_Lists
from database_oracle import connect_to_oracle, connect_to_oracle_test, connect_to_oracle_gl_posting
import cx_Oracle
from log_file import createFolder
import json
from abc import ABC, abstractmethod

async def getGroupDetailsReport_code(cnx,company_id,bu_id,plant_id,department_id,equipment_group_id,equipment_class_id=''):

    sql = ''' SELECT
			mt.*
		FROM
            master_equipment mt
		WHERE mt.status <> 'delete' '''

    if company_id != "":
        sql += f" and mt.company_id =  '{company_id}' "
		
    if bu_id != "" :
        sql += f"and mt.bu_id =  '{bu_id}' "

    if plant_id != "" :
        sql += f"and mt.plant_id =  '{plant_id}' "

    if department_id != "" :
        sql += f"and mt.plant_department_id =  '{department_id}' "

    if equipment_group_id != "" :
        sql += f"and mt.equipment_group_id =  '{equipment_group_id}' "

    if equipment_class_id != "" :
        sql += f"and mt.equipment_class_id =  '{equipment_class_id}' "

    sql += '''Order By mt.equipment_id '''

    result = await cnx.execute(text(sql))
    result = result.fetchall()

    return result

# async def get_item_dtl_from_oracle_old(cnx,PTYPE):
    
#     connection_oracle_data = connect_to_oracle()

#     cursor = connection_oracle_data.cursor()
#     DESPATCH_RSET = cursor.var(cx_Oracle.CURSOR)
    
#     query = "BEGIN APPS.TI_IIOT_PKG.GET_DESPATCH_DATA ( :P_TYPE, :P_FROM_DATE, :DESPATCH_RSET ); END;"
#     cursor.execute(query, P_TYPE=PTYPE, P_FROM_DATE=P_FROMDATE,DESPATCH_RSET=DESPATCH_RSET)
#     result_cursor = DESPATCH_RSET.getvalue()

#     if result_cursor is not None:
#         column_names = [desc[0] for desc in result_cursor.description]
#         createFolder("oracle_data/",f"column_names-{column_names}")
#         JOB_RSET_LIST = []

    

#         for row in result_cursor: 

#             oracle_end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
#             data = {}
#             for i, column_name in enumerate(column_names):
            
#                 data[column_name] = row[i]                
#             JOB_RSET_LIST.append(data)
#             createFolder("oracle_data/",f"data-{i_data}")
#         print(count)
#     cursor.close()
#     connection_oracle_data.close() 
#     #return result_cursor
# ?====
async def get_item_dtl_from_oracle(cnx,P_TYPE):
    createFolder(f"oracle_data/{P_TYPE}/",f"Function Call P_TYPE {P_TYPE}")
    if P_TYPE == 'MONTH':
        sql = "select ifnull(max(LAST_UPDATE_DATE),'0001-01-01 00:00:00') P_FROM_DATE from master_despatch_month "

        createFolder(f"oracle_data/{P_TYPE}/",f"if P_TYPE == 'MONTH':")
    elif P_TYPE == 'DAY':
        sql = "select ifnull(max(LAST_UPDATE_DATE),'0001-01-01 00:00:00') P_FROM_DATE  from master_despatch_day "
        createFolder(f"oracle_data/{P_TYPE}/",f"elif P_TYPE == 'DAY':")
    data = await cnx.execute(sql)
    data = data.fetchall()
    P_FROM_DATE = '0001-01-01 00:00:00'
    createFolder(f"oracle_data/{P_TYPE}/",f"executed query{sql}")
    createFolder(f"oracle_data/{P_TYPE}/",f"output{data}")
    
    if len(data)>0:
        for i in data:
            P_FROM_DATE = i["P_FROM_DATE"]
        createFolder(f"oracle_data/{P_TYPE}/",f"if len(data)>0:")
   
    connection_oracle_data = connect_to_oracle()

    cursor = connection_oracle_data.cursor()
    DESPATCH_RSET = cursor.var(cx_Oracle.CURSOR)
   
    query = "BEGIN APPS.TI_IIOT_PKG.GET_DESPATCH_DATA(:P_TYPE, :P_FROM_DATE, :DESPATCH_RSET); END;"

    cursor.execute(query, 
                    P_TYPE=P_TYPE,
                    P_FROM_DATE = P_FROM_DATE,
                    DESPATCH_RSET=DESPATCH_RSET
                    )
    
    
    result_cursor = DESPATCH_RSET.getvalue()
    
    # createFolder("oracle_data/",f"executed query {query}")
    # createFolder("oracle_data/",f"inputs for query  {P_TYPE, P_FROM_DATE, DESPATCH_RSET}")
    # createFolder("oracle_data/",f"output:  {result_cursor}")
    # createFolder("oracle_data/",f"output:  {len(result_cursor)}")
    if result_cursor is not None:
        column_names = [desc[0] for desc in result_cursor.description]
        createFolder(f"oracle_data/{P_TYPE}/",f"if result_cursor is not None:")
        for data in result_cursor: 
            i_data = {}
            for i, column_name in enumerate(column_names):
                if data[i] == None:
                    data_val = ''
                    createFolder(f"oracle_data/{P_TYPE}/",f"if data[i] == None:")
                else:
                    data_val = data[i]  
                    createFolder(f"oracle_data/{P_TYPE}/",f" if data[i] == None: else:")       
                i_data[column_name] = data_val
            createFolder(f"oracle_data/{P_TYPE}/",f"data -- {i_data}")
            if P_TYPE == 'MONTH':
                select_query = f" select * from master_despatch_month where MONTH_REC_ID = '{i_data['MONTH_REC_ID']}' "
                sel_data = await cnx.execute(select_query)
                sel_data = sel_data.fetchall()
                createFolder(f"oracle_data/{P_TYPE}/",f"if P_TYPE == 'MONTH':") 
                if len(sel_data)>0:
                    query1 = f""" Update master_despatch_month set
                    SHIP_FROM_ORGN_ID = '{i_data['SHIP_FROM_ORGN_ID']}' ,
                    INVENTORY_ITEM_ID = '{i_data['INVENTORY_ITEM_ID']}',
                    ORDERED_UOM_CODE = '{i_data['ORDERED_UOM_CODE']}', 
                    INVOICE_QTY = '{i_data['INVOICE_QTY']}', 
                    INVOICE_QTY_MTR = '{i_data['INVOICE_QTY_MTR']}',
                    INVOICE_QTY_NOS = '{i_data['INVOICE_QTY_NOS']}',
                    INVOICE_QTY_TON = '{i_data['INVOICE_QTY_TON']}',
                    INVOICE_QTY_EFT = '{i_data['INVOICE_QTY_EFT']}',
                    INVOICE_QTY_KGS = '{i_data['INVOICE_QTY_KGS']}',
                    DESPATCH_MONTH = '{i_data['DESPATCH_MONTH']}', 
                    CREATION_DATE = '{i_data['CREATION_DATE']}',
                    LAST_UPDATE_DATE = '{i_data['LAST_UPDATE_DATE']}',
                    ORGANIZATION_CODE = '{i_data['ORGANIZATION_CODE']}',
                    ITEM_CODE = '{i_data['ITEM_CODE']}',
                    ITEM_CAT1 = '{i_data['ITEM_CAT1']}',
                    ITEM_CAT2 = '{i_data['ITEM_CAT2']}',
                    modified_on = now()
                    where MONTH_REC_ID = '{i_data['MONTH_REC_ID']}' """
                    createFolder(f"oracle_data/{P_TYPE}/",f"if len(sel_data)>0:") 
                else:

                    query1 = f""" INSERT INTO master_despatch_month (MONTH_REC_ID,SHIP_FROM_ORGN_ID,INVENTORY_ITEM_ID, ORDERED_UOM_CODE, INVOICE_QTY, INVOICE_QTY_MTR, INVOICE_QTY_NOS, INVOICE_QTY_TON, INVOICE_QTY_EFT,INVOICE_QTY_KGS, DESPATCH_MONTH, CREATION_DATE,LAST_UPDATE_DATE,ORGANIZATION_CODE,ITEM_CODE,ITEM_CAT1,ITEM_CAT2,created_on)
                    value ('{i_data['MONTH_REC_ID']}','{i_data['SHIP_FROM_ORGN_ID']}','{i_data['INVENTORY_ITEM_ID']}', '{i_data['ORDERED_UOM_CODE']}', '{i_data['INVOICE_QTY']}', '{i_data['INVOICE_QTY_MTR']}', '{i_data['INVOICE_QTY_NOS']}', '{i_data['INVOICE_QTY_TON']}', '{i_data['INVOICE_QTY_EFT']}',
                    '{i_data['INVOICE_QTY_KGS']}', '{i_data['DESPATCH_MONTH']}', '{i_data['CREATION_DATE']}', '{i_data['LAST_UPDATE_DATE']}','{i_data['ORGANIZATION_CODE']}','{i_data['ITEM_CODE']}','{i_data['ITEM_CAT1']}','{i_data['ITEM_CAT2']}',now())"""
                    createFolder(f"oracle_data/{P_TYPE}/",f" if len(sel_data)>0: else:") 
                await cnx.execute(query1)
                await cnx.commit()
                createFolder(f"oracle_data/{P_TYPE}/",f"despatch Month wise data insert done!!!")

            elif P_TYPE == 'DAY':   
                select_query = f" select * from master_despatch_day where DAY_REC_ID = '{i_data['DAY_REC_ID']}' "
                sel_data = await cnx.execute(select_query)
                sel_data = sel_data.fetchall()
                createFolder(f"oracle_data/{P_TYPE}/",f" elif P_TYPE == 'DAY':  ") 
                if len(sel_data)>0:
                    query1 = f""" Update master_despatch_day set
                    SHIP_FROM_ORGN_ID = '{i_data['SHIP_FROM_ORGN_ID']}' ,
                    INVENTORY_ITEM_ID = '{i_data['INVENTORY_ITEM_ID']}',
                    ORDERED_UOM_CODE = '{i_data['ORDERED_UOM_CODE']}', 
                    INVOICE_QTY = '{i_data['INVOICE_QTY']}', 
                    INVOICE_QTY_MTR = '{i_data['INVOICE_QTY_MTR']}',
                    INVOICE_QTY_NOS = '{i_data['INVOICE_QTY_NOS']}',
                    INVOICE_QTY_TON = '{i_data['INVOICE_QTY_TON']}',
                    INVOICE_QTY_EFT = '{i_data['INVOICE_QTY_EFT']}',
                    INVOICE_QTY_KGS = '{i_data['INVOICE_QTY_KGS']}',
                    DESPATCH_DATE = '{i_data['DESPATCH_DATE']}', 
                    CREATION_DATE = '{i_data['CREATION_DATE']}',
                    LAST_UPDATE_DATE = '{i_data['LAST_UPDATE_DATE']}',
                    ORGANIZATION_CODE = '{i_data['ORGANIZATION_CODE']}',
                    ITEM_CODE = '{i_data['ITEM_CODE']}',
                    ITEM_CAT1 = '{i_data['ITEM_CAT1']}',
                    ITEM_CAT2 = '{i_data['ITEM_CAT2']}',
                    modified_on = now()
                    where DAY_REC_ID = '{i_data['DAY_REC_ID']}' """
                    createFolder(f"oracle_data/{P_TYPE}/",f"despatch Day wise data Update done!!!")
                    createFolder(f"oracle_data/{P_TYPE}/",f" if len(sel_data)>0: ") 
                else:
                    query1 = f""" INSERT INTO master_despatch_day (DAY_REC_ID,SHIP_FROM_ORGN_ID,INVENTORY_ITEM_ID, ORDERED_UOM_CODE, INVOICE_QTY, INVOICE_QTY_MTR, INVOICE_QTY_NOS, INVOICE_QTY_TON, INVOICE_QTY_EFT,INVOICE_QTY_KGS, DESPATCH_DATE, CREATION_DATE,LAST_UPDATE_DATE,ORGANIZATION_CODE,ITEM_CODE,ITEM_CAT1,ITEM_CAT2, created_on)
                    value ('{i_data['DAY_REC_ID']}','{i_data['SHIP_FROM_ORGN_ID']}','{i_data['INVENTORY_ITEM_ID']}', '{i_data['ORDERED_UOM_CODE']}', '{i_data['INVOICE_QTY']}', '{i_data['INVOICE_QTY_MTR']}', '{i_data['INVOICE_QTY_NOS']}', '{i_data['INVOICE_QTY_TON']}', '{i_data['INVOICE_QTY_EFT']}',
                    '{i_data['INVOICE_QTY_KGS']}', '{i_data['DESPATCH_DATE']}', '{i_data['CREATION_DATE']}', '{i_data['LAST_UPDATE_DATE']}','{i_data['ORGANIZATION_CODE']}','{i_data['ITEM_CODE']}','{i_data['ITEM_CAT1']}','{i_data['ITEM_CAT2']}',now())"""
                    createFolder(f"oracle_data/{P_TYPE}/",f"despatch Day wise data insert done!!!")
                    createFolder(f"oracle_data/{P_TYPE}/",f" if len(sel_data)>0: else: ") 
                await cnx.execute(query1)
                await cnx.commit()
                
    else:
       createFolder(f"oracle_data/{P_TYPE}/",f"No data available in result_cursor")   
       createFolder(f"oracle_data/{P_TYPE}/",f" if result_cursor is not None: else: ")                            
    cursor.close()
    connection_oracle_data.close() 
      
async def get_item_dtl_from_oracle_manual(cnx,P_TYPE):
    
    if P_TYPE == 'MONTH':
        sql = "select ifnull(max(LAST_UPDATE_DATE),'0001-01-01 00:00:00') P_FROM_DATE from master_despatch_month "
        createFolder(f"oracle_data/{P_TYPE}/",f"if P_TYPE == 'MONTH': ") 
    elif P_TYPE == 'DAY' :
        sql = "select ifnull(max(LAST_UPDATE_DATE),'0001-01-01 00:00:00') P_FROM_DATE  from master_despatch_day "
        createFolder(f"oracle_data/{P_TYPE}/",f"elif P_TYPE == 'DAY' :") 
    data = await cnx.execute(sql)
    data = data.fetchall()
    P_FROM_DATE = '0001-01-01 00:00:00'
    
    if len(data)>0:
        for i in data:
            P_FROM_DATE = i["P_FROM_DATE"]
        createFolder(f"oracle_data/{P_TYPE}/",f"if len(data)>0:") 
   
    connection_oracle_data = connect_to_oracle()

    cursor = connection_oracle_data.cursor()
    DESPATCH_RSET = cursor.var(cx_Oracle.CURSOR)
   
    query = "BEGIN APPS.TI_IIOT_PKG.GET_DESPATCH_DATA(:P_TYPE, :P_FROM_DATE, :DESPATCH_RSET); END;"

    cursor.execute(query, 
                    P_TYPE=P_TYPE,
                    P_FROM_DATE = P_FROM_DATE,
                    DESPATCH_RSET=DESPATCH_RSET
                    )
    
    
    result_cursor = DESPATCH_RSET.getvalue()
    if result_cursor is not None:
        column_names = [desc[0] for desc in result_cursor.description]
        createFolder(f"oracle_data/{P_TYPE}/",f"if result_cursor is not None:") 
        for data in result_cursor: 
            i_data = {}
            for i, column_name in enumerate(column_names):
                if data[i] == None:
                    data_val = ''
                    createFolder(f"oracle_data/{P_TYPE}/",f"if data[i] == None:") 
                else:
                    data_val = data[i]         
                    createFolder(f"oracle_data/{P_TYPE}/",f"if data[i] == None: else:") 
                i_data[column_name] = data_val
            createFolder(f"oracle_data/{P_TYPE}/",f"data -- {i_data}")
            if P_TYPE == 'MONTH':
                
                query1 = f""" INSERT INTO master_despatch_month (MONTH_REC_ID,SHIP_FROM_ORGN_ID,INVENTORY_ITEM_ID, ORDERED_UOM_CODE, INVOICE_QTY, INVOICE_QTY_MTR, INVOICE_QTY_NOS, INVOICE_QTY_TON, INVOICE_QTY_EFT,INVOICE_QTY_KGS, DESPATCH_MONTH, CREATION_DATE,LAST_UPDATE_DATE,ORGANIZATION_CODE,ITEM_CODE,ITEM_CAT1,ITEM_CAT2)
                value ('{i_data['MONTH_REC_ID']}','{i_data['SHIP_FROM_ORGN_ID']}','{i_data['INVENTORY_ITEM_ID']}', '{i_data['ORDERED_UOM_CODE']}', '{i_data['INVOICE_QTY']}', '{i_data['INVOICE_QTY_MTR']}', '{i_data['INVOICE_QTY_NOS']}', '{i_data['INVOICE_QTY_TON']}', '{i_data['INVOICE_QTY_EFT']}',
                '{i_data['INVOICE_QTY_KGS']}', '{i_data['DESPATCH_MONTH']}', '{i_data['CREATION_DATE']}', '{i_data['LAST_UPDATE_DATE']}','{i_data['ORGANIZATION_CODE']}','{i_data['ITEM_CODE']}','{i_data['ITEM_CAT1']}','{i_data['ITEM_CAT2']}')"""
                await cnx.execute(query1)
                await cnx.commit()
                createFolder(f"oracle_data/{P_TYPE}/",f"despatch Month wise data insert done!!!")
                createFolder(f"oracle_data/{P_TYPE}/",f"if P_TYPE == 'MONTH':") 
            elif P_TYPE == 'DAY':   
                
                query1 = f""" INSERT INTO master_despatch_day (DAY_REC_ID,SHIP_FROM_ORGN_ID,INVENTORY_ITEM_ID, ORDERED_UOM_CODE, INVOICE_QTY, INVOICE_QTY_MTR, INVOICE_QTY_NOS, INVOICE_QTY_TON, INVOICE_QTY_EFT,INVOICE_QTY_KGS, DESPATCH_DATE, CREATION_DATE,LAST_UPDATE_DATE,ORGANIZATION_CODE,ITEM_CODE,ITEM_CAT1,ITEM_CAT2)
                value ('{i_data['DAY_REC_ID']}','{i_data['SHIP_FROM_ORGN_ID']}','{i_data['INVENTORY_ITEM_ID']}', '{i_data['ORDERED_UOM_CODE']}', '{i_data['INVOICE_QTY']}', '{i_data['INVOICE_QTY_MTR']}', '{i_data['INVOICE_QTY_NOS']}', '{i_data['INVOICE_QTY_TON']}', '{i_data['INVOICE_QTY_EFT']}',
                '{i_data['INVOICE_QTY_KGS']}', '{i_data['DESPATCH_DATE']}', '{i_data['CREATION_DATE']}', '{i_data['LAST_UPDATE_DATE']}','{i_data['ORGANIZATION_CODE']}','{i_data['ITEM_CODE']}','{i_data['ITEM_CAT1']}','{i_data['ITEM_CAT2']}')"""
            
                await cnx.execute(query1)
                await cnx.commit()
                createFolder(f"oracle_data/{P_TYPE}/",f"despatch Day wise data insert done!!!")
                createFolder(f"oracle_data/{P_TYPE}/",f"elif P_TYPE == 'DAY': ") 
    else:
       createFolder(f"oracle_data/{P_TYPE}/",f"No data available in result_cursor")   
       createFolder(f"oracle_data/{P_TYPE}/",f"if result_cursor is not None: else: ")                            
    cursor.close()
    connection_oracle_data.close()   


# tj
# async def post_ems_account_entry(data: list):
   
    # Connect to Oracle asynchronously
    connection = connect_to_oracle()
    # data = [{'ac': '401.499.194.440001.211199.999.999', 'desc': 'Variable power cost Provision Provided (Tube)\n- MAY-2024', 'dr': 15594903, 'cr': 0}, {'ac': '401.499.194.440001.211199.999.999', 'desc': 'Variable power cost Provision Provided (TCD)\n- MAY-2024', 'dr': 1352671, 'cr': 0}, {'ac': '402.402.194.440001.211199.999.999', 'desc': 'Variable power cost Provision Provided (Strips- WW/NW)\n- MAY-2024', 'dr': 8369153, 'cr': 0}, {'ac': '402.405.194.440001.211199.999.999', 'desc': 'Variable power cost Provision Provided (Strips-Service)\n- MAY-2024', 'dr': 860820, 'cr': 0}, {'ac': '201.201.152.440002.999999.999.999', 'desc': 'Variable power cost Provision Provided (Corp R & D)\n- MAY-2024', 'dr': 67913, 'cr': 0}, {'ac': '421.404.194.440001.211299.999.999', 'desc': 'Variable power cost Provision Provided (IBD)\n- MAY-2024', 'dr': 8691742, 'cr': 0}, {'ac': '421.404.194.440002.211299.999.999', 'desc': 'Fixed power cost Provision Provided (IBD)\n- MAY-2024', 'dr': 1906248, 'cr': 0}, {'ac': '201.201.152.440002.999999.999.999', 'desc': 'Fixed power cost Provision Provided (Corp R & D)\n- MAY-2024', 'dr': 14894, 'cr': 0}, {'ac': '402.402.194.440002.211199.999.999', 'desc': 'Fixed power cost Provision Provided (Strips- WW/NW)\n- MAY-2024', 'dr': 2024292, 'cr': 0}, {'ac': '401.499.194.440002.211199.999.999', 'desc': 'Fixed power cost Provision Provided (Tube)\n- MAY-2024', 'dr': 3716894, 'cr': 0}, {'ac': '401.499.999.251444.211199.999.999', 'desc': 'Power cost / Provision Provided for\n- MAY-2024', 'dr': 0, 'cr': 42599534}, {'ac': '201.201.152.440002.999999.999.999', 'desc': 'Power Captive Generation Corp. R&D\n- MAY-2024', 'dr': 2655, 'cr': 0}, {'ac': '421.404.999.440003.211299.999.999', 'desc': 'Power Captive Generation IBP\n- MAY-2024', 'dr': 339894, 'cr': 0}, {'ac': '402.402.562.440003.211199.999.999', 'desc': 'Power Captive Generation Service Center\n- MAY-2024', 'dr': 33662, 'cr': 0}, {'ac': '402.402.177.440003.211199.999.999', 'desc': 'Power Captive Generation N/W & WW\n\n- MAY-2024', 'dr': 327279, 'cr': 0}, {'ac': '401.499.177.440003.211199.999.999', 'desc': 'Power Captive Generation allocation to other units\n- MAY-2024', 'dr': 0, 'cr': 703493}, {'ac': '401.499.177.440003.211199.999.999', 'desc': 'Power Captive Generation tax prov 10 paise per unit\n- MAY-2024', 'dr': 3795, 'cr': 0}, {'ac': '401.499.999.251444.211199.999.999', 'desc': 'Power Captive Generation tax prov 10 paise per unit\n- MAY-2024', 'dr': 0, 'cr': 3795}]

    # Prepare the JSON data
    p_obj_json = json.dumps({"ems": data})
    
    # Call the Oracle procedure
    cursor = connection.cursor()
    cursor.callproc("APPS.TI_IIOT_PKG.POST_EMS_JV", [p_obj_json, cursor])
    
    # Commit the transaction
    connection.commit()
    
    # Fetch the output record set
    jv_rset = cursor.fetchall()
    
    connection.close()
    
    # Process the output record set
    output = {}
    for row in jv_rset:
        error_code, error_message, remarks = row
        if error_code == 0:
            output = {
                "Error Code": error_code,
                "Error Message": error_message,
                "Remarks": remarks,
                "Status": "Success"
            }
            createFolder("oracle_data/",f"if error_code == 0: ")    
        else:
            output = {
                "Error Code": error_code,
                "Error Message": error_message,
                "Remarks": remarks,
                "Status": "Failed"
            }
            createFolder("oracle_data/",f"if error_code == 0: else: ")    
    
    return output
    
    
    
import logging
import json
import asyncio
import cx_Oracle

# Configure logging settings
logging.basicConfig(filename='app.log', 
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

async def post_ems_account_entry(p_obj_json):
    createFolder("oracle_data/",f"step 1")
       
    connection_oracle_data = connect_to_oracle_gl_posting()
    createFolder("oracle_data/",f"step 2")

    cursor = connection_oracle_data.cursor()
    JV_RSET = cursor.var(cx_Oracle.CURSOR)
   
    query = "BEGIN APPS.TI_IIOT_PKG.POST_EMS_JV(:P_OBJ_JSON, :JV_RSET); END;"

    cursor.execute(query, P_OBJ_JSON = p_obj_json, JV_RSET=JV_RSET)
    createFolder("oracle_data/",f"step 3")
    
    result_cursor = JV_RSET.getvalue()
    createFolder("oracle_data/",f"step 4")
    i_data = {}
    
    
    if result_cursor is not None:
        createFolder("oracle_data/",f"step 5")
        column_names = [desc[0] for desc in result_cursor.description]

        for data in result_cursor: 
            for i, column_name in enumerate(column_names):
                if data[i] == None:
                    data_val = ''
                else:
                    data_val = data[i]
                    
                if column_name == 'ERROR_CODE':
                    if data_val == 0:
                        i_data["Status"] = "Success"
                    else:
                        i_data["Status"] = "Failed"
                       
                i_data[column_name] = data_val
            
                
            createFolder("oracle_data/",f"data -- {i_data}")  
            
            # {'Status': 'Failed', 'ERROR_CODE': -6, 'ERROR_MSG': 'Total of Debit and Credit values are not matching'}
         
    else:
        createFolder("oracle_data/",f"step 6")
        createFolder("oracle_data/",f"No data available in result_cursor")                              
    
    cursor.close()
    connection_oracle_data.close() 
    createFolder("oracle_data/",f"returning data -- {i_data}")  
    
    return i_data



class DataProcessor(ABC):

    @abstractmethod
    async def get_max_last_update_date(self, cnx, P_TYPE):
        pass

    @abstractmethod
    async def insert_or_update_data(self, cnx, P_TYPE, data):
        pass

    @abstractmethod
    async def process_data(self, cnx, P_TYPE):
        pass

class OracleDataProcessor(DataProcessor):

    async def get_max_last_update_date(self, cnx, P_TYPE):
       
        sql = f'''SELECT 
                        IFNULL(MAX(LAST_UPDATE_DATE), '0001-01-01 00:00:00') AS P_FROM_DATE 
                FROM 
                    {'master_despatch_month' if P_TYPE == 'MONTH' else 'master_despatch_day'}
                '''
        data = await cnx.execute(sql)
        data = data.fetchall()
        P_FROM_DATE = '0001-01-01 00:00:00'
        if len(data) > 0:
            P_FROM_DATE = data[0]["P_FROM_DATE"]
        return P_FROM_DATE
    
    async def insert_or_update_data(self, cnx, P_TYPE, data):
        try:
            if P_TYPE == 'DAY':
                query =  f'''
                            INSERT INTO 
                                    master_despatch_day (
                                    DAY_REC_ID, SHIP_FROM_ORGN_ID, INVENTORY_ITEM_ID, ORDERED_UOM_CODE, 
                                    INVOICE_QTY, INVOICE_QTY_MTR, INVOICE_QTY_NOS, INVOICE_QTY_TON, INVOICE_QTY_EFT, 
                                    INVOICE_QTY_KGS, DESPATCH_DATE, CREATION_DATE, LAST_UPDATE_DATE, ORGANIZATION_CODE, 
                                    ITEM_CODE, ITEM_CAT1, ITEM_CAT2, created_on
                                    )
                            VALUES({data['DAY_REC_ID']}, {data['SHIP_FROM_ORGN_ID']}, {data['INVENTORY_ITEM_ID']}, '{data['ORDERED_UOM_CODE']}', {data['INVOICE_QTY']}, 
                                    {data['INVOICE_QTY_MTR']}, {data['INVOICE_QTY_NOS']}, {data['INVOICE_QTY_TON']},'{ data['INVOICE_QTY_EFT']}', '{data['INVOICE_QTY_KGS']}',   
                                    '{data['DESPATCH_DATE'].strftime('%Y-%m-%d %H:%M:%S')}', '{data['CREATION_DATE'].strftime('%Y-%m-%d %H:%M:%S')}', '{data['LAST_UPDATE_DATE'].strftime('%Y-%m-%d %H:%M:%S')}', '{data['ORGANIZATION_CODE']}', '{data['ITEM_CODE']}',
                                    '{data['ITEM_CAT1']}', '{data['ITEM_CAT2']}', NOW() 
                                    )
                            ON DUPLICATE KEY UPDATE
                                SHIP_FROM_ORGN_ID = VALUES(SHIP_FROM_ORGN_ID),
                                INVENTORY_ITEM_ID = VALUES(INVENTORY_ITEM_ID),
                                ORDERED_UOM_CODE = VALUES(ORDERED_UOM_CODE),
                                INVOICE_QTY = VALUES(INVOICE_QTY),
                                INVOICE_QTY_MTR = VALUES(INVOICE_QTY_MTR),
                                INVOICE_QTY_NOS = VALUES(INVOICE_QTY_NOS),
                                INVOICE_QTY_TON = VALUES(INVOICE_QTY_TON),
                                INVOICE_QTY_EFT = VALUES(INVOICE_QTY_EFT),
                                INVOICE_QTY_KGS = VALUES(INVOICE_QTY_KGS),
                                DESPATCH_DATE = VALUES(DESPATCH_DATE),
                                CREATION_DATE = VALUES(CREATION_DATE),
                                LAST_UPDATE_DATE = VALUES(LAST_UPDATE_DATE),
                                ORGANIZATION_CODE = VALUES(ORGANIZATION_CODE),
                                ITEM_CODE = VALUES(ITEM_CODE),
                                ITEM_CAT1 = VALUES(ITEM_CAT1),
                                ITEM_CAT2 = VALUES(ITEM_CAT2),
                                modified_on = NOW()
                        '''
            elif P_TYPE == 'MONTH':
                query =  f'''
                            INSERT INTO 
                                    master_despatch_month (
                                    MONTH_REC_ID, SHIP_FROM_ORGN_ID, INVENTORY_ITEM_ID, ORDERED_UOM_CODE, 
                                    INVOICE_QTY, INVOICE_QTY_MTR, INVOICE_QTY_NOS, INVOICE_QTY_TON, INVOICE_QTY_EFT, 
                                    INVOICE_QTY_KGS, DESPATCH_MONTH, CREATION_DATE, LAST_UPDATE_DATE, ORGANIZATION_CODE, 
                                    ITEM_CODE, ITEM_CAT1, ITEM_CAT2, created_on
                                    )
                            VALUES({data['MONTH_REC_ID']}, {data['SHIP_FROM_ORGN_ID']}, {data['INVENTORY_ITEM_ID']}, '{data['ORDERED_UOM_CODE']}', {data['INVOICE_QTY']}, 
                                    {data['INVOICE_QTY_MTR']}, {data['INVOICE_QTY_NOS']}, {data['INVOICE_QTY_TON']}, '{data['INVOICE_QTY_EFT']}', '{data['INVOICE_QTY_KGS']}',   
                                    '{data['DESPATCH_MONTH'].strftime('%Y%m')}', '{data['CREATION_DATE'].strftime('%Y-%m-%d %H:%M:%S')}', '{data['LAST_UPDATE_DATE'].strftime('%Y-%m-%d %H:%M:%S')}', '{data['ORGANIZATION_CODE']}', '{data['ITEM_CODE']}',
                                    '{data['ITEM_CAT1']}', '{data['ITEM_CAT2']}', NOW() 
                                    )
                            ON DUPLICATE KEY UPDATE
                                SHIP_FROM_ORGN_ID = VALUES(SHIP_FROM_ORGN_ID),
                                INVENTORY_ITEM_ID = VALUES(INVENTORY_ITEM_ID),
                                ORDERED_UOM_CODE = VALUES(ORDERED_UOM_CODE),
                                INVOICE_QTY = VALUES(INVOICE_QTY),
                                INVOICE_QTY_MTR = VALUES(INVOICE_QTY_MTR),
                                INVOICE_QTY_NOS = VALUES(INVOICE_QTY_NOS),
                                INVOICE_QTY_TON = VALUES(INVOICE_QTY_TON),
                                INVOICE_QTY_EFT = VALUES(INVOICE_QTY_EFT),
                                INVOICE_QTY_KGS = VALUES(INVOICE_QTY_KGS),
                                DESPATCH_MONTH = VALUES(DESPATCH_MONTH),
                                CREATION_DATE = VALUES(CREATION_DATE),
                                LAST_UPDATE_DATE = VALUES(LAST_UPDATE_DATE),
                                ORGANIZATION_CODE = VALUES(ORGANIZATION_CODE),
                                ITEM_CODE = VALUES(ITEM_CODE),
                                ITEM_CAT1 = VALUES(ITEM_CAT1),
                                ITEM_CAT2 = VALUES(ITEM_CAT2),
                                modified_on = NOW()
                        '''

        
                        
            createFolder(f"oracle_data/{P_TYPE}/", f"data----->{data}")
            
            createFolder(f"oracle_data/{P_TYPE}/", f"insert_or_update_data: query  --->{query}")
            await cnx.execute(query)


            createFolder(f"oracle_data/{P_TYPE}/", f"insert_or_update_data: query executed --->{query}")
            await cnx.commit()
            createFolder(f"oracle_data/{P_TYPE}/", f"insert_or_update_data: query commited successfully --->{query}")
            
             
        except Exception as e:
            createFolder(f"oracle_data/{P_TYPE}/",f" insert_or_update_data : {str(e)}")
    
    # async def insert_or_update_data(self, cnx, P_TYPE, data):
    #     try:
    #         if P_TYPE == 'DAY':
    #             query = '''
    #                 INSERT INTO master_despatch_day_new (
    #                     DAY_REC_ID, SHIP_FROM_ORGN_ID, INVENTORY_ITEM_ID, ORDERED_UOM_CODE, 
    #                     INVOICE_QTY, INVOICE_QTY_MTR, INVOICE_QTY_NOS, INVOICE_QTY_TON, INVOICE_QTY_EFT, 
    #                     INVOICE_QTY_KGS, DESPATCH_DATE, CREATION_DATE, LAST_UPDATE_DATE, ORGANIZATION_CODE, 
    #                     ITEM_CODE, ITEM_CAT1, ITEM_CAT2, created_on
    #                 )
    #                 VALUES (:DAY_REC_ID, :SHIP_FROM_ORGN_ID, :INVENTORY_ITEM_ID, :ORDERED_UOM_CODE, 
    #                         :INVOICE_QTY, :INVOICE_QTY_MTR, :INVOICE_QTY_NOS, :INVOICE_QTY_TON, :INVOICE_QTY_EFT, 
    #                         :INVOICE_QTY_KGS, :DESPATCH_DATE, :CREATION_DATE, :LAST_UPDATE_DATE, :ORGANIZATION_CODE, 
    #                         :ITEM_CODE, :ITEM_CAT1, :ITEM_CAT2, NOW())
    #                 ON DUPLICATE KEY UPDATE
    #                     SHIP_FROM_ORGN_ID = VALUES(SHIP_FROM_ORGN_ID),
    #                     INVENTORY_ITEM_ID = VALUES(INVENTORY_ITEM_ID),
    #                     ORDERED_UOM_CODE = VALUES(ORDERED_UOM_CODE),
    #                     INVOICE_QTY = VALUES(INVOICE_QTY),
    #                     INVOICE_QTY_MTR = VALUES(INVOICE_QTY_MTR),
    #                     INVOICE_QTY_NOS = VALUES(INVOICE_QTY_NOS),
    #                     INVOICE_QTY_TON = VALUES(INVOICE_QTY_TON),
    #                     INVOICE_QTY_EFT = VALUES(INVOICE_QTY_EFT),
    #                     INVOICE_QTY_KGS = VALUES(INVOICE_QTY_KGS),
    #                     DESPATCH_DATE = VALUES(DESPATCH_DATE),
    #                     CREATION_DATE = VALUES(CREATION_DATE),
    #                     LAST_UPDATE_DATE = VALUES(LAST_UPDATE_DATE),
    #                     ORGANIZATION_CODE = VALUES(ORGANIZATION_CODE),
    #                     ITEM_CODE = VALUES(ITEM_CODE),
    #                     ITEM_CAT1 = VALUES(ITEM_CAT1),
    #                     ITEM_CAT2 = VALUES(ITEM_CAT2),
    #                     modified_on = NOW()
    #             '''
    #         elif P_TYPE == 'MONTH':
    #             query = f'''
    #                 INSERT INTO master_despatch_month_new (
    #                     MONTH_REC_ID, SHIP_FROM_ORGN_ID, INVENTORY_ITEM_ID, ORDERED_UOM_CODE, 
    #                     INVOICE_QTY, INVOICE_QTY_MTR, INVOICE_QTY_NOS, INVOICE_QTY_TON, INVOICE_QTY_EFT, 
    #                     INVOICE_QTY_KGS, DESPATCH_MONTH, CREATION_DATE, LAST_UPDATE_DATE, ORGANIZATION_CODE, 
    #                     ITEM_CODE, ITEM_CAT1, ITEM_CAT2, created_on
    #                 )
    #                 VALUES (:MONTH_REC_ID, :SHIP_FROM_ORGN_ID, :INVENTORY_ITEM_ID, :ORDERED_UOM_CODE, 
    #                         :INVOICE_QTY, :INVOICE_QTY_MTR, :INVOICE_QTY_NOS, :INVOICE_QTY_TON, :INVOICE_QTY_EFT, 
    #                         :INVOICE_QTY_KGS, :DESPATCH_MONTH, :CREATION_DATE, :LAST_UPDATE_DATE, :ORGANIZATION_CODE, 
    #                         :ITEM_CODE, :ITEM_CAT1, :ITEM_CAT2, NOW())
    #                 ON DUPLICATE KEY UPDATE
    #                     SHIP_FROM_ORGN_ID = VALUES(SHIP_FROM_ORGN_ID),
    #                     INVENTORY_ITEM_ID = VALUES(INVENTORY_ITEM_ID),
    #                     ORDERED_UOM_CODE = VALUES(ORDERED_UOM_CODE),
    #                     INVOICE_QTY = VALUES(INVOICE_QTY),
    #                     INVOICE_QTY_MTR = VALUES(INVOICE_QTY_MTR),
    #                     INVOICE_QTY_NOS = VALUES(INVOICE_QTY_NOS),
    #                     INVOICE_QTY_TON = VALUES(INVOICE_QTY_TON),
    #                     INVOICE_QTY_EFT = VALUES(INVOICE_QTY_EFT),
    #                     INVOICE_QTY_KGS = VALUES(INVOICE_QTY_KGS),
    #                     DESPATCH_MONTH = VALUES(DESPATCH_MONTH),
    #                     CREATION_DATE = VALUES(CREATION_DATE),
    #                     LAST_UPDATE_DATE = VALUES(LAST_UPDATE_DATE),
    #                     ORGANIZATION_CODE = VALUES(ORGANIZATION_CODE),
    #                     ITEM_CODE = VALUES(ITEM_CODE),
    #                     ITEM_CAT1 = VALUES(ITEM_CAT1),
    #                     ITEM_CAT2 = VALUES(ITEM_CAT2),
    #                     modified_on = NOW()
    #             '''
    #             values = {
    #                 'MONTH_REC_ID': data.get('DAY_REC_ID'),
    #                 'SHIP_FROM_ORGN_ID': data.get('SHIP_FROM_ORGN_ID'), 
    #                 'INVENTORY_ITEM_ID': data.get('INVENTORY_ITEM_ID'),
    #                 'ORDERED_UOM_CODE': data.get('ORDERED_UOM_CODE'),
    #                 'INVOICE_QTY': data.get('INVOICE_QTY'),
    #                 'INVOICE_QTY_MTR': data.get('INVOICE_QTY_MTR'),
    #                 'INVOICE_QTY_NOS': data.get('INVOICE_QTY_NOS'),
    #                 'INVOICE_QTY_TON': data.get('INVOICE_QTY_TON'),
    #                 'INVOICE_QTY_EFT': data.get('INVOICE_QTY_EFT', ''),  
    #                 'INVOICE_QTY_KGS': data.get('INVOICE_QTY_KGS', ''), 
    #                 'DESPATCH_MONTH': data.get('DESPATCH_MONTH').strftime('%Y-%m-%d %H:%M:%S') if data.get('DESPATCH_MONTH') else '',
    #                 'CREATION_DATE': data.get('CREATION_DATE').strftime('%Y-%m-%d %H:%M:%S') if data.get('CREATION_DATE') else '',
    #                 'LAST_UPDATE_DATE': data.get('LAST_UPDATE_DATE').strftime('%Y-%m-%d %H:%M:%S') if data.get('LAST_UPDATE_DATE') else '',
    #                 'ORGANIZATION_CODE': data.get('ORGANIZATION_CODE'),
    #                 'ITEM_CODE': data.get('ITEM_CODE'),
    #                 'ITEM_CAT1': data.get('ITEM_CAT1'),
    #                 'ITEM_CAT2': data.get('ITEM_CAT2')
    #             }

    #         # values = {
    #         #     data.get('DAY_REC_ID',  data.get('MONTH_REC_ID')),
    #         #     data['SHIP_FROM_ORGN_ID'], 
    #         #     data['INVENTORY_ITEM_ID'],
    #         #     data['ORDERED_UOM_CODE'],
    #         #     data['INVOICE_QTY'],
    #         #     data['INVOICE_QTY_MTR'],
    #         #     data['INVOICE_QTY_NOS'],
    #         #     data['INVOICE_QTY_TON'],
    #         #     data.get('INVOICE_QTY_EFT') or '',  
    #         #     data.get('INVOICE_QTY_KGS') or '', 
    #         #     data.get('DESPATCH_DATE', data.get('DESPATCH_MONTH')).strftime('%Y-%m-%d %H:%M:%S'),
    #         #     data['CREATION_DATE'].strftime('%Y-%m-%d %H:%M:%S'),
    #         #     data['LAST_UPDATE_DATE'].strftime('%Y-%m-%d %H:%M:%S'),
    #         #     data['ORGANIZATION_CODE'],
    #         #     data['ITEM_CODE'],
    #         #     data['ITEM_CAT1'],
    #         #     data['ITEM_CAT2']
    #         # }
            
    #         # values = {
    #         #     data.get('DAY_REC_ID',  data.get('MONTH_REC_ID')),
    #         #     data['SHIP_FROM_ORGN_ID'], 
    #         #     data['INVENTORY_ITEM_ID'],
    #         #     data['ORDERED_UOM_CODE'],
    #         #     data['INVOICE_QTY'],
    #         #     data['INVOICE_QTY_MTR'],
    #         #     data['INVOICE_QTY_NOS'],
    #         #     data['INVOICE_QTY_TON'],
    #         #     data.get('INVOICE_QTY_EFT') or '',  
    #         #     data.get('INVOICE_QTY_KGS') or '', 
    #         #     data.get('DESPATCH_DATE', data.get('DESPATCH_MONTH')).strftime('%Y-%m-%d %H:%M:%S'),
    #         #     data['CREATION_DATE'].strftime('%Y-%m-%d %H:%M:%S'),
    #         #     data['LAST_UPDATE_DATE'].strftime('%Y-%m-%d %H:%M:%S'),
    #         #     data['ORGANIZATION_CODE'],
    #         #     data['ITEM_CODE'],
    #         #     data['ITEM_CAT1'],
    #         #     data['ITEM_CAT2']
    #         # }
            
    #         createFolder(f"oracle_data/{P_TYPE}/", f"data----->{data}")
    #         createFolder(f"oracle_data/{P_TYPE}/", f"values----->{values}")
    #         createFolder(f"oracle_data/{P_TYPE}/", f"insert_or_update_data: query  --->{query}")

    #         await cnx.execute(query, values)

    #         createFolder(f"oracle_data/{P_TYPE}/", f"insert_or_update_data: query executed --->{query}")
    #         await cnx.commit()
    #         createFolder(f"oracle_data/{P_TYPE}/", f"insert_or_update_data: query commited successfully --->{query}")
    #     except Exception as e:
    #         createFolder(f"oracle_data/{P_TYPE}/", f"insert_or_update_data: {str(e)}")

    async def process_data(self, cnx, P_TYPE):
        try:
            createFolder(f"oracle_data/{P_TYPE}", f"Function Call P_TYPE {P_TYPE}")
            P_FROM_DATE = await self.get_max_last_update_date(cnx, P_TYPE)
            createFolder(f"oracle_data/{P_TYPE}", f"executed query to get max last update date,{P_FROM_DATE}")

            connection_oracle_data = connect_to_oracle()
            cursor = connection_oracle_data.cursor()
            DESPATCH_RSET = cursor.var(cx_Oracle.CURSOR)

            query = "BEGIN APPS.TI_IIOT_PKG.GET_DESPATCH_DATA(:P_TYPE, :P_FROM_DATE, :DESPATCH_RSET); END;"
            cursor.execute(query, P_TYPE=P_TYPE, P_FROM_DATE=P_FROM_DATE, DESPATCH_RSET=DESPATCH_RSET)
            result_cursor = DESPATCH_RSET.getvalue()
            createFolder(f"oracle_data/{P_TYPE}", f"oracle data fetch query executed--->{query}")

            if result_cursor is not None:
                column_names = [desc[0] for desc in result_cursor.description]
                for data in result_cursor:
                    i_data = {column_names[i]: (data[i] if data[i] is not None else '') for i in range(len(column_names))}

                    await self.insert_or_update_data(cnx, P_TYPE, i_data)
                createFolder(f"oracle_data/{P_TYPE}/",f"<<-loop Completed->>") 
            else:
                createFolder(f"oracle_data/{P_TYPE}/",f"No data available in result_cursor") 
            cursor.close()
            connection_oracle_data.close()
        except Exception as e:
            createFolder(f"oracle_data/{P_TYPE}/",f"process_data: {str(e)}") 

 