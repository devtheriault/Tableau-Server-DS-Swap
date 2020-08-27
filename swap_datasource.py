import os
import shutil
import tsc_logic
import xml.etree.ElementTree as ET 
import tableauserverclient as TSC
from tableauserverclient import ConnectionCredentials, ConnectionItem
from tableaudocumentapi import Connection, xfile
from tableaudocumentapi.xfile import xml_open
from tableaudocumentapi import Datasource

datasource_list = []
project_id_list = []
project_list = []

# dbnames
old_dbname = 'OLD_DBNAME'
new_dbname = 'NEW_DBNAME'

# Tableau Server credentials URL format: 'https://tableau.servername.com/', site format: 'site-name'
tableau_server_url = 'TABLEAU_SERVER_URL'
tableau_server_username = 'TABLEAU_SERVER_USERNAME'
tableau_server_password = 'TABLEAU_SERVER_PASSWORD'
tableau_server_site = 'TABLEAU_SERVER_SITE'

# Snowflake credentials
snowflake_username = 'SNOWFLAKE_USERNAME'
snowflake_password = 'SNOWFLAKE_PASSWORD'

tableau_auth = TSC.TableauAuth(tableau_server_username, tableau_server_password, tableau_server_site)
server = TSC.Server(tableau_server_url)
connection_credentials = TSC.ConnectionCredentials(snowflake_username, snowflake_password, embed=True)

def create_temp_dir():
    try:
        os.mkdir(os.getcwd() + "/DownloadedFiles/")
    except OSError:
        print("Failed to create directory")
    else:
        print("made")

def cleanup(tempdir):
    shutil.rmtree(tempdir)

def publish_datasource():
    try:
        xfile._save_file(datasource_path, datasource_tree)
        datasource_name = item.split('.')[0]
        index = datasource_list.index(datasource_name)
        project_name = project_list[index].name
        # Publishing Datasource
        tsc_logic.publish_datasource(server, datasource_name, project_name, datasource_path, connection_credentials)
    except OSError:
        print("Failed to save and publish datasource:", datasource_name)

with server.auth.sign_in(tableau_auth):

    # Spliting datasources into seperate lists
    datasources = tsc_logic.get_all_datasources(server)
    for datasource in datasources:
        datasource_list.append(datasource.name)
        project_id_list.append(datasource.project_id)
    
    # Creating project_list from project_id_list
    for project in project_id_list:
        project_list.append(tsc_logic.select_a_project_with_id(server, project))

    create_temp_dir()

    # Downloading all datasources from Tableau Server to /DownloadedFiles/ dir
    tsc_logic.download_datasource_list(server, datasource_list)

    # Creating a list of files from each file in /DownloadedFiles/ dir
    for dirpath, dirnames, files in os.walk(os.getcwd() + "/DownloadedFiles/"):
        files

    print('\n')

    # Iterating through each .tds or .tdsx file in the "files" list
    for item in files:
        if ".tds" in item:
            # Converting .tds/.tdsx files to xml
            datasource_path = os.getcwd() + "/DownloadedFiles/" + item
            datasource_tree = xml_open(datasource_path, 'datasource')
            datasource_root = datasource_tree.getroot()

            for connection in datasource_root.iter('connection'):
                # Changing database
                if connection.attrib['class'] == 'snowflake' and connection.attrib['dbname'] == old_dbname and old_dbname in connection.attrib['one-time-sql']:
                    connection.attrib['dbname'] = new_dbname
                    connection.attrib['one-time-sql'].replace(old_dbname, new_dbname)

                    publish_datasource()

                elif connection.attrib['class'] == 'snowflake' and connection.attrib['dbname'] == old_dbname:
                    connection.attrib['dbname'] = new_dbname

                    publish_datasource()

                # This will log a list of any Snowflake connections that are using a different server/dbname
                elif connection.attrib['class'] == 'snowflake':
                    print(item.split('.')[0], "is connected to this db: \n", connection.attrib['server'], connection.attrib['dbname'])

    # Deleting tempdir
    cleanup('DownloadedFiles/')