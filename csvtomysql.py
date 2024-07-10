import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import requests

def read_data_from_google_sheet():
    csv_url = "https://docs.google.com/spreadsheets/d/1iUlbwQym-DXTjLG-j6cyAzRTFeBRTvfTqHD2c8yEduQ/gviz/tq?tqx=out:csv"
    # csv_url = "https://docs.google.com/spreadsheets/d/1iUlbwQym-DXTjLG-j6cyAzRTFeBRTvfTqHD2c8yEduQ/export?format=csv&id=16LfzET9kV-Jk6Sa5ljGBwXnjQ_yQnU4QwWdqIQNkEXA&gid=2088744909"
    df = pd.read_csv(csv_url)
    # df = pd.DataFrame(df)
    # print(df.columns)
    return df

def insert_rxtype(read_data):
    df = pd.DataFrame(read_data)

    rx_columns = ['Seen Rx (Obj)', 'Cholbe Rx (Obj)', 'Indoor & Discharge Rx (Obj)', 'Chamber Rx (Obj)']

    df_processed = df.melt(
        id_vars=["MSOID", "Objective Date"],
        value_vars=rx_columns,
        var_name='rx_desc',
        value_name='rxtype_obj'
    )

    df_processed['Objective Date'] = pd.to_datetime(df_processed['Objective Date'], format='%m-%d-%Y', errors='coerce')
    
    sql_data=''
    obj_date=''
    for rowId, row in df_processed.iterrows():
        
        obj_date = row['Objective Date'].strftime('%Y-%m-%d') if pd.notna(row['Objective Date']) else "NULL"
        if str(obj_date)=="NULL":
            continue
        if sql_data=='':
            sql_data='INSERT INTO mso_obj_rxtype (msoid,obj_date,rx_desc,rxtype_obj) values ("'+str(row['MSOID'])+'","'+str(obj_date)+'","'+str(row['rx_desc'])+'","'+str(row['rxtype_obj'])+'")'
        else:
            sql_data+=',("'+str(row['MSOID'])+'","'+str(obj_date)+'","'+str(row['rx_desc'])+'","'+str(row['rxtype_obj'])+'")'
    
    cursor.execute(sql_data)
    conn.commit()
    return True

def insert_brand_data(read_data):
    df = pd.DataFrame(read_data)

    brand_columns = ["Rx Obj. >> Biltin >> Input Number", "Rx Obj. >> Nabumet >> Input Number", "Rx Obj. >> Ostocal Gx >> Input Number", "Rx Obj. >> Kefuclav >> Input Number", "Rx Obj. >> Elmood >> Input Number", "Rx Obj. >> Esoral Mups  >> Input Number", "Rx Obj. >> Folimax >> Input Number","Rx Obj. >> Lumona >> Input Number", "Rx Obj. >> Mig >> Input Number", "Rx Obj. >> Vitamax D >> Input Number",  "Rx Obj. >> Triject >> Input Number","Rx Obj. >> Meroject >> Input Number", "Rx Obj. >> Enorin >> Input Number", "Rx Obj. >> Filgram >> Input Number","Rx Obj. (Eye) >> Zymarin >> Input Number","Rx Obj. (Eye) >> Napolin >> Input Number", "Rx Obj. (Eye) >> Resight >> Input Number", "Rx Obj. (Eye) >> Retears Liquigel >> Input Number","Rx Obj. (Eye) >> Retears >> Input Number","Rx Obj. (Eye) >> Visovit >> Input Number","Rx Obj. (Eye) >> Aladay Max >> Input Number", "Rx Obj. (Eye) >> Zolopt >> Input Number","Rx Obj. (Eye) >> Aladay DS >> Input Number", "Rx Obj. (Eye) >> Freshtear >> Input Number", "Rx Obj. (Eye) >> Visomox OPT >> Input Number","Rx Obj. (Eye) >> Aladay >> Input Number", "Rx Obj. (Eye) >> Levomax OS >> Input Number", "Rx Obj. (Eye) >> Romfen >> Input Number","Rx Obj. (Eye) >> Binzotim >> Input Number", "Rx Obj. (Eye) >> Freshtear P >> Input Number","Rx Obj. (Hormone) >> Reomen >> Input Number", "Rx Obj. (Hormone) >> Endolix >> Input Number", "Rx Obj. (Hormone) >> Cetrolix >> Input Number", "Rx Obj. (Hormone) >> Floren >> Input Number","Rx Obj. (Hormone) >> Ethinor >> Input Number","Rx Obj. (Hormone) >> Tibonor >> Input Number", "Rx Obj. (Hormone) >> Danamet >> Input Number", "Rx Obj. (Hormone) >> Endosis >> Input Number", "Rx Obj. (Hormone) >> Femikit >> Input Number", "Rx Obj. (Hormone) >> Thynor >> Input Number", "Rx Obj. (Hormone) >> Lenor >> Input Number", "Rx Obj. (Team C) >> Emazid >> Input Number", "Rx Obj. (Team C) >> Tems >> Input Number", "Rx Obj. (Team C) >> Cardobis >> Input Number", "Rx Obj. (Team C) >> Ligazid >> Input Number", "Rx Obj. (Team C) >> Cifibet >> Input Number", "Rx Obj. (Team C) >> Noclog >> Input Number", "Rx Obj. (Team C) >> Creston >> Input Number", "Rx Obj. (Team C) >> Oroxat >> Input Number", "Rx Obj. (Team C) >> Dephos >> Input Number", "Rx Obj. (Team C) >> Arnis >> Input Number", "Rx Obj. (Team C) >> Simpress/Cilny >> Input Number", "Rx Obj. (Team C) >> Duoliv >> Input Number", "Chemist Coverage Obj. >> Tufnil >> Input Number","Chemist Coverage Obj. >> Xinc B Syp >> Input Number", "Chemist Coverage Obj. >> Alben Tab >> Input Number", "Rx Obj. (Team C) >> Enorin >> Input Number"]

        # Melt the DataFrame to transform it into the desired format
    df_processed = df.melt(id_vars=["MSOID", "Objective Date"],
                               value_vars=brand_columns,
                               var_name='brand_desc',
                               value_name='brand_obj'
                               )


    df_processed['Objective Date'] = pd.to_datetime(df_processed['Objective Date'], format='%m-%d-%Y', errors='coerce')
    
    sql_data=''
    obj_date=''
    for rowId, row in df_processed.iterrows():
        
        obj_date = row['Objective Date'].strftime('%Y-%m-%d') if pd.notna(row['Objective Date']) else "NULL"
        if str(obj_date)=="NULL":
            continue
        if sql_data=='':
            sql_data='INSERT INTO mso_obj_brand (msoid,obj_date,brand_desc,brand_obj) values ("'+str(row['MSOID'])+'","'+str(obj_date)+'","'+str(row['brand_desc'])+'","'+str(row['brand_obj'])+'")'
        else:
            sql_data+=',("'+str(row['MSOID'])+'","'+str(obj_date)+'","'+str(row['brand_desc'])+'","'+str(row['brand_obj'])+'")'

    cursor.execute(sql_data)
    conn.commit()
    return True

def insert_item_data(read_data):
    df = pd.DataFrame(read_data)

    item_columns = ["Chemist Coverage Obj. >> Losectil 20 mg Cap 120's - 1784 >> Input Number", "Chemist Coverage Obj. >> Losectil 20 mg Cap 300's - 1967 >> Input Number", "Chemist Coverage Obj. >> Dexpoten Plus - 1717 >> Input Number", "Chemist Coverage Obj. >> Xinc 20MG Tab - 1820 >> Input Number", "Chemist Coverage Obj. >> Xinc Syp 100 - 1087 >> Input Number", "Chemist Coverage Obj. >> Flucloxin 500mg Cap - 1576 >> Input Number", "Chemist Coverage Obj. >> Ostocal D 500mg Tab 30's - 1219 >> Input Number", "Chemist Coverage Obj. >> Alben SUS 200mg - 1101 >> Input Number"]
    # Melt the DataFrame to transform it into the desired format
    df_processed = df.melt(id_vars=["MSOID", "Objective Date"],
                               value_vars=item_columns,
                               var_name='item_desc',
                               value_name='item_obj'
                               )


    df_processed['Objective Date'] = pd.to_datetime(df_processed['Objective Date'], format='%m-%d-%Y', errors='coerce')
    
    sql_data=''
    obj_date=''
    for rowId, row in df_processed.iterrows():
        
        obj_date = row['Objective Date'].strftime('%Y-%m-%d') if pd.notna(row['Objective Date']) else "NULL"
        if str(obj_date)=="NULL":
            continue
        if sql_data=='':
            sql_data='INSERT INTO mso_obj_item (msoid,obj_date,item_desc,item_obj) values ("'+str(row['MSOID'])+'","'+str(obj_date)+'","'+str(row['item_desc'])+'","'+str(row['item_obj'])+'")'
        else:
            sql_data+=',("'+str(row['MSOID'])+'","'+str(obj_date)+'","'+str(row['item_desc'])+'","'+str(row['item_obj'])+'")'
    
    cursor.execute(sql_data)
    conn.commit()
    return True

def insert_mso_obj(read_data):
    df = pd.DataFrame(read_data)

    # read_data = read_data[['MSOID', 'MSONAME', 'MSOTR', 'MSOGROUP', 'FMTR', 'RSMTR', 'TEAM', 'Entry Date', 'Objective Date', 'Sales Objective Final', 'No of Chemist']].copy()


    df['Objective Date'] = pd.to_datetime(df['Objective Date'], format='%m-%d-%Y', errors='coerce')
    df['Entry Date'] = pd.to_datetime(df['Entry Date'], format='%m-%d-%Y', errors='coerce')
    
    sql_data=''
    obj_date=''
    entry_date=''
    for rowId, row in df.iterrows():
        obj_date = row['Objective Date'].strftime('%Y-%m-%d') if pd.notna(row['Objective Date']) else "NULL"
        entry_date = row['Entry Date'].strftime('%Y-%m-%d') if pd.notna(row['Entry Date']) else "NULL"
        if str(obj_date)=="NULL":
            continue
        if str(entry_date)=="NULL":
            continue
        if sql_data=='':
            sql_data='INSERT INTO mso_obj (msoid, msoname, mso_tr, msogroup, fm_tr, rsm_tr, team, entry_date, obj_date, sales_obj, no_of_chem_obj) values ("'+str(row['MSOID'])+'","'+str(row['MSONAME'])+'","'+str(row['MSOTR'])+'","'+str(row['MSOGROUP'])+'","'+str(row['FMTR'])+'","'+str(row['RSMTR'])+'","'+str(row['TEAM'])+'","'+str(entry_date)+'","'+str(obj_date)+'","'+str(row['Sales Objective Final'])+'","'+str(row['No of Chemist'])+'")'
        else:
            sql_data+=',("'+str(row['MSOID'])+'","'+str(row['MSONAME'])+'","'+str(row['MSOTR'])+'","'+str(row['MSOGROUP'])+'","'+str(row['FMTR'])+'","'+str(row['RSMTR'])+'","'+str(row['TEAM'])+'","'+str(entry_date)+'","'+str(obj_date)+'","'+str(row['Sales Objective Final'])+'","'+str(row['No of Chemist'])+'")'
    
    cursor.execute(sql_data)
    conn.commit()
    return True

if __name__ == "__main__":
    try:
        # Establish a connection to the database
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            database='skf_rep_process'
        )
        cursor = conn.cursor()
        # Establish a connection to the database

        # read from csv
        read_data=read_data_from_google_sheet()

        # Insert into rxtype table
        rxtype=insert_rxtype(read_data)
        if rxtype==True:
            print("RXTYPE Data successfully inserted!")

        # Insert into brand table
        brand=insert_brand_data(read_data)
        if brand==True:
            print("Brand Data successfully inserted!")

        # Insert into item table
        item=insert_item_data(read_data)
        if item==True:
            print("Item Data successfully inserted!")

        # Insert into item table
        mso=insert_mso_obj(read_data)
        if mso==True:
            print("MSO Data successfully inserted!")
        



    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)

