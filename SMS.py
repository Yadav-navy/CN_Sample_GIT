import pandas as pd
import re
import mysql.connector as mysql

def change_format_SMS(input_path,output_path,id):
    try:
        df = pd.read_excel(input_path)
        df_new = df['Remark'].astype(str) + "; " +df['Created Time'].astype(str)
        df_new = pd.DataFrame(df_new)
        
        columns = ['Package Id','Package Name', 'Channel Name','Operation Date','Action' ]
        result = []
        
        response = 0
        
        for row in df_new[0]:
            row = re.sub(r' +', ' ', row)
            row = re.sub(r';{2,}', ';', row)

            product_id_match = re.search(r'(?:To|From|to|from) Product \[(\d+)\]([^;]+);', row)
            if product_id_match:
                product_id= int(product_id_match.group(1))
                product_name = product_id_match.group(2).strip()
            else:
                product_id=""
                product_name=""


            channels_deleted_match = re.search(r'Removed Program(.*?)(\s+From Product)', row)
            channels_deleted = channels_deleted_match.group(1).split(',') if channels_deleted_match else []

            channels_added_match = re.search(r'Added Program(.*?)(\s+to Product)', row)
            channels_added = channels_added_match.group(1).split(',') if channels_added_match else []

            split_values = row.split(';')
            time = split_values[-1].strip()
                  

            if product_id:
                for channel in channels_added:
                    result.append([product_id,product_name, channel, time, 'Added'])
                for channel in channels_deleted:
                    result.append([product_id,product_name, channel, time, 'Removed'])
            else:
                result.append(["", "", "", ""])

        df = pd.DataFrame(result, columns=columns)
        df.replace('', pd.NA, inplace=True)
        df.replace(' ', pd.NA, inplace=True)
        df.dropna(subset=['Channel Name'], inplace=True)
        
        df.to_excel(output_path, index=False)
        response = 1
        
        if response == 1:
            try:
                config = {'user': 'root', 'password': 'PrQL:?0He9ZzK#3', 'host': '192.168.167.61',
                              'port': '3306', 'database': 'trai'}
                    
                connection = mysql.connect(**config)
                    
                query = "UPDATE trai_file_download_details SET file_status = TRUE WHERE id = '" + str(id) + "';"
                cursor = connection.cursor()
                cursor.execute(query)
                connection.commit()
                    
                #print("File status updated successfully")
                
            except mysql.connector.Error as mysql_error:
                return
        
    except Exception as e:
        return

