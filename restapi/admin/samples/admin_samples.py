# get the admin subpackage of restapi
import os
import sys
sys.path.append(os.path.abspath('...'))
from restapi import admin
import restapi
import datetime
from dateutil.relativedelta import relativedelta

def main(url, usr, pw, folder_name, service_name):
    """run tests against your own servers

    url -- url to internal ArcGIS Server instance (must be inside network)
    usr -- administrative username for Server
    pw -- administrative password for Server
    folder_name = name of a folder to test (will only make temp security/configured state changes)
    service_name = full service name or wild card matching a service name, should include just enough for a
        unique query.  Will only temporarily make minor changes.
        
        wild card ex:
            # full service name is "SampleWorldCities.MapServer"
            service_name = "sampleworldcities" # not case sensative, should find the servide
    """
    # connect to ArcGIS Server instance
    arcserver = admin.ArcServerAdmin(url, usr, pw)

    #-----------------------------------------------------------------------------------------------#
    # list services and configured state in a single folder
    folder = arcserver.folder(folder_name)
    for service in folder.iter_services():
        print service.serviceName, service.configuredState

        # can stop a service like this
        # service.stop()

        # or start like this
        # service.start()

    print '\n' * 3

    # show all services and configured state (use iter_services to return restapi.admin.Service() object!)
    for service in arcserver.iter_services():
        print service.serviceName, service.configuredState
        
    print '\n' * 3

    #-----------------------------------------------------------------------------------------------#
    # setting security on a folder
    # make a folder publically available (i.e. unsecure it)
    arcserver.addPermission(folder_name, private=False) # can also do this from a Folder object

    # this is now unsecured, let's secure it again
    arcserver.addPermission(folder_name)  # by default it will make private True (sets security)

    #-----------------------------------------------------------------------------------------------#
    # stop all services in a folder
    arcserver.stopServices(folderName=folder_name) # this can take a few minutes

    # look thru the folder to check the configured states, should be stopped
    for service in folder.iter_services():
        print service.serviceName, service.configuredState

    # now restart
    arcserver.startServices(folderName=folder_name) # this can take a few minutes

    # look thru folder, services should be started
    for service in folder.iter_services():
        print service.serviceName, service.configuredState

    #-----------------------------------------------------------------------------------------------#
    # query log files (within last 3 days), need to convert to milliseconds
    threeDaysAgo = restapi.date_to_mil(datetime.datetime.now() - relativedelta(days=3))
    for log in arcserver.queryLogs(startTime=threeDaysAgo, pageSize=25):
        print(log.time)
        for message in log:
            print(message)
        print('\n')

    #-----------------------------------------------------------------------------------------------#
    # connect to an individual service (by wildcard) - do not need to include full name, just
    # enough of the name to make it a unique name query
    service = arcserver.service(service_name)

    # get original service description
    description = service.description

    # now edit the description
    service.edit(description='This is an updated service description')

    # edit description again to set it back to the original description
    service.edit(description=description)

    #-----------------------------------------------------------------------------------------------#
    # connect to the server's data store
    ds = arcserver.dataStore

    # iterate through all items of data store
    for item in ds:
        print item.type, item.path
        # if it is an enterprise database connection, you can get the connection string like this
        if item.type == 'egdb':
            print(item.info.connectionString)
        # else if a folder, print server path
        elif item.type == 'folder':
            print(item.info.path)
        print('\n')


if __name__ == '__main__':

    # test with your own servers
    url = 'localhost:6080/arcgis/admin/services' #server url
    usr = 'username'
    pw = 'password'

    folder_name = 'SOME_FOLDER'
    service_name = 'Service_Wildcard'

    # run all tests
    main(url, usr, pw, folder_name, service_name)
