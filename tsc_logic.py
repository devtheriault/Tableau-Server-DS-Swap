import tableauserverclient as TSC
from tableauserverclient import ConnectionCredentials, ConnectionItem
import os


def select_item_from_list(list, item_name):
    count = 0 
    for item in list:
        if item.name == item_name:
            selected_item = list[count]
            return selected_item
        count += 1

def select_item_from_list_id(list, item_id):
    count = 0 
    for item in list:
        if item.id == item_id:
            selected_item = list[count]
            return selected_item
        count += 1

def select_a_project(server, project_name):
    projects = get_all_projects(server)
    selected_project = select_item_from_list(projects, project_name)
    return selected_project

def select_a_project_with_id(server, project_id):
    projects = get_all_projects(server)
    selected_project = select_item_from_list_id(projects, project_id)
    return selected_project

def get_all_projects(server):
    all_projects_items, pagination_item = server.projects.get()
    return all_projects_items

def select_datasource(server, datasource_name):
    datasource_list = get_all_datasources(server)
    selected_datasource = select_item_from_list(datasource_list, datasource_name)
    return selected_datasource

def get_all_datasources(server):
    all_datasources, pagination_item = server.datasources.get()
    return all_datasources

def download_datasource_list(server, datasource_names):
    for item in datasource_names:
        selected_datasource = select_datasource(server, item)
        server.datasources.download(selected_datasource.id, filepath= os.getcwd() + "/DownloadedFiles/", include_extract=False)
        print(selected_datasource.name, "has been downloaded")

def select_workbook(server, workbook_name):
    workbook_list = get_all_workbooks(server)
    selected_workbook = select_item_from_list(workbook_list, workbook_name)
    return selected_workbook

def get_all_workbooks(server):
    all_workbooks, pagination_item = server.workbooks.get()
    return all_workbooks

def download_workbook_list(server, workbook_names):
    for item in workbook_names:
        print(item)
        selected_workbook = select_workbook(server, item)
        server.workbooks.download(selected_workbook.id, filepath= os.getcwd() + "/DownloadedFiles/")
        print(selected_workbook.name, "has been downloaded")
    return selected_workbook

def create_project(server, project_name):
    new_project = TSC.ProjectItem(name=project_name, content_permissions="ManagedByOwner", description='')
    try:
        print("Attempting to create new project")
        server.projects.create(new_project)
        print(project_name, "has been created successfully ")
    except ConnectionError:
        print(ConnectionError, "Project failed to be created")
    
def publish_workbook(server, project_name, workbook_name, path):
    publish_mode = TSC.Server.PublishMode.Overwrite
    project = select_a_project(server, project_name)
    new_workbook = TSC.WorkbookItem(name=workbook_name, project_id= project.id)
    new_workbook = server.workbooks.publish(new_workbook, path, publish_mode)

    print("Workbook published. ID: {}".format(new_workbook.id))

def publish_datasource(server, datasource_name, project_name, path, connection_credentials):
    publish_mode = TSC.Server.PublishMode.Overwrite
    project = select_a_project(server, project_name)
    new_datasource = TSC.DatasourceItem(name=datasource_name, project_id=project.id)
    new_datasource = server.datasources.publish(new_datasource, path, publish_mode, connection_credentials)

    print("Datasource published. ID: {}".format(new_datasource.id))

# Takes in a workbook object and the path of the workbook and publishes it
def replace_workbook(server, workbook, path):
    publish_mode = TSC.Server.PublishMode.Overwrite
    workbook = TSC.WorkbookItem(name=workbook.name, project_id=workbook.project_id)
    workbook = server.workbooks.publish(workbook, path, publish_mode)
    print("Workbook datasource has been swapped. ID: {}".format(workbook.id))
