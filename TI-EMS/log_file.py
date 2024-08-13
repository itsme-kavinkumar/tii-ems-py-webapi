import os
import sys
from datetime import datetime, timedelta

def createFolder(directory, data):
    date_time = datetime.now()
    curtime1 = date_time.strftime("%d/%m/%Y %H:%M:%S")
    curtime2 = date_time.strftime("%d-%m-%Y")

    try:
        # Get the path of the current script
        base_path = os.path.abspath(os.path.dirname(sys.argv[0]))

        # Create the directory inside the user's file directory
        directory = os.path.join(base_path, directory)
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Remove log files older than 5 days
        five_days_ago = date_time - timedelta(days=5)
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            if os.path.isfile(file_path):
                file_date_str = filename.split('.')[0]
                try:
                    file_date = datetime.strptime(file_date_str, "%d-%m-%Y")
                except ValueError:
                    # Skip files that cannot be parsed as dates in the expected format
                    continue
                if file_date < five_days_ago:
                    os.remove(file_path)

        # Create the log file inside the directory
        file_path = os.path.join(directory, f"{curtime2}.txt")
        with open(file_path, "a+") as f:
            f.write(f"{curtime1} {data}\r\n")
    except OSError as e:
        print(f"Error: Creating directory. {directory} - {e}")

# Example usage
createFolder("Log/", "Issue in returning data")


# def CreateFolder_cron_log(cnx, drive, title="", content=""):
#     try:
#         drive_value = drive(cnx)
#         for i in drive_value:
#             drive_data = i["drive_name"]
#         dir_name = "Api_Log"
#         curtime1=datetime.now().strftime("%d/%m/%Y %H:%M:%S")
#         curtime2=datetime.now().strftime("%d-%m-%Y")
#         write_content = f"Date - Time : {curtime1} title : {title} content : {content}"
#         drive_data = drive_data +":/" +"AIC"
#         if not os.path.exists(drive_data):
#             os.makedirs(drive_data)
#         if not os.path.exists(drive_data + "/"+ dir_name):
#             os.makedirs(drive_data + "/"+ dir_name)
#         write_to_file = open(drive_data + "/"+ dir_name + "/" + curtime2 + ".txt", "a+")
#         write_to_file.write(write_content)
#         write_to_file.close()
        
#     except OSError:
#         print ('Error: Creating directory. ' +  drive_data)