import pandas as pd
import re
import mysql.connector as mysql

print("CAS Format")

def change_format_CAS(input_path, output_path, id):
    try:
        # path : path for excel file
        df = pd.read_excel(input_path)
        new_header = df.iloc[0]
        df = df[1:]
        df.columns = new_header
        df = df[['Operate Detail Info']].copy()
        
        columns = ['Package Id', 'Channel Name', 'Operation Date', 'Action']
        result = []
        
        response = 0
        
        for row in df['Operate Detail Info']:
            product_id_match = re.search(r'Product id:(\d+)', row)
            product_id = product_id_match.group(1) if product_id_match else None

            channels_added_match = re.search(r'ADD:(.*?)(\.DEL:|\.Time:)', row)
            channels_added = channels_added_match.group(1).split('.') if channels_added_match else []

            channels_deleted_match = re.search(r'DEL:(.*?)\s+Time:', row)
            channels_deleted = channels_deleted_match.group(1).split('.') if channels_deleted_match else []

            time_match = re.search(r'(?<=Time:)(.*)', row)
            time = time_match.group(1) if time_match else None

            if product_id:
                for channel in channels_added:
                    result.append([product_id, channel, time, 'Added'])
                for channel in channels_deleted:
                    result.append([product_id, channel, time, 'Removed'])
            else:
                result.append(["", "", "", ""])
        
        df = pd.DataFrame(result, columns=columns)
        df.replace('', pd.NA, inplace=True)
        df.dropna(subset=['Channel Name'], inplace=True)
        
        df.insert(1, "Package Name", "")  # adding column for Package Name
        
        df.to_excel(output_path, index=False)
        
        response = 1
        #print(response)
        
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
