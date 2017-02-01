# Will be performing a screen scrape
#   Has login requirement therefore use Selenium
import MySQLdb
from MySQLdb.connections import Connection as mysqlConnection
from sshtunnel import SSHTunnelForwarder
from selenium import webdriver
from selenium.webdriver.common.by import By as WebDriver_By
from selenium.webdriver.support.ui import WebDriverWait as WebDriver_Wait
from selenium.webdriver.support import expected_conditions as WebDriver_ExpCond
from selenium.common import exceptions as WebDriver_Except
from time import sleep
from time import time
from time import strftime
import datetime
import copy
import pandas
import paramiko
import os
import math
import numpy



#       Need geckodriver
geckoDriver_path = "C:\Python\Libraries\geckodriver\geckodriver_64bit.exe"

# Define website URL
url = 'https://researchpac.hbs.edu/cacti/'

# Define location of login info
cfg_login_folderpath = "C:\\Users\\pjonak\\Documents\\keys\\"
cfg_login_filepath = "pacConfig.txt"

# Define
cfg_grid_folderpath = "C:\\Users\\pjonak\\Documents\\keys\\"
cfg_grid_filepath = "gridConfig.txt"

cfg_db_folderpath = "C:\\Users\\pjonak\\Documents\\keys\\"
cfg_db_filepath = "dbConfig.txt"

db_tableName = "cactiScrape"



def dbInsert(dat: pandas.core.frame.DataFrame) -> bool:
    # Step 1) Connect to databsae
    # Step 2) Build SQL command
    #   a) Determine if table already exists
    #           if table does not exist
    #               a.i)   determine column names and column data types
    #               a.ii)  build SQL command to create table
    #               a.iii) execute SQL command
    #               a.iv)  verify
    #   b) Determine column names and ordering
    #   c) Determine if any data needs to be converted (e.g. string value needs to be integer value)
    #
    # Step 3) Execute SQL command


    # #############################
    # Step 1)
    # #############################
    cfg_grid = pandas.read_csv(cfg_grid_folderpath + cfg_grid_filepath)
    cfg_db = pandas.read_csv(cfg_db_folderpath + cfg_db_filepath)

    with SSHTunnelForwarder(
            (cfg_grid["value"]["host"], int(cfg_grid["value"]["port"])),
            ssh_password=cfg_grid["value"]["password"],
            ssh_username=cfg_grid["value"]["username"],
            remote_bind_address=(
                    cfg_db["value"]["host"],
                    int(cfg_db["value"]["port"]))) as server:

        I_conn = False
        try:
            hConn = MySQLdb.connect(host=cfg_db['value']['host'],
                                    database=cfg_db['value']['database'],
                                    user=cfg_db['value']['username'],
                                    password=cfg_db['value']['password'])
            I_conn = True
        except:
            print("Error connecting to database")
            print("\tUnable to insert data into database")

        if I_conn:
            I_conn = False
            # Pretty sure we should be in the database but just in case verify it is in the information schema
            cmdStr = buildSQL_existDB(cfg_db['value']['database'])
            if pandas.read_sql(cmdStr, hConn).empty:
                print("Error: Connected to server but unable to access database")

                # Close connection
                hConn.close()

            else:
                I_conn = True

        if I_conn:
            # #############################
            # Step 2)
            # #############################


            # #############################
            # Step 2.a) Check if table is in the database
            # #############################
            I_haveTable = False

            cmdStr = buildSQL_existTable(cfg_db['value']['database'], db_tableName)
            if pandas.read_sql(cmdStr, hConn).empty:
                # No, table does not exist in the specified database
                print("\tTable does not exist - Creating...")

                # Need to create table
                cmdStr = buildSQL_createTable(cfg_db['value']['database'], db_tableName, dat)
                print(dispSQL(cmdStr))

                hCursor = hConn.cursor()
                hCursor.execute(cmdStr)
                res = hCursor.fetchall()
                if len(res)==0:
                    print("\tSuccess")

                    # Verify

                    I_haveTable = True

                else:
                    print("\tError:")
                    print("\t\tresult length: " + str(len(res)))
                    print(res)
                    print("\n")
            else:
                I_haveTable = True

            if not I_haveTable:
                print("\tError with table: " + db_tableName)
                print("\t\tUnable to proceed")
            else:
                # #############################
                # Step 2.b) Determine column names and ordering
                # #############################
                # Should have been handled at this point
                #   columns names in "dat" are the same name and order as in the database
                #
                # Except the first column in the database, EntryID
                #   This is an auto-incrementing primary key
                #   We don't need to provide it


                # #############################
                # Step 2.c) Determine if any data needs to be converted
                # #############################
                # From the function buildSQL_createTable we see that yes there is some conversion
                # However, this occurs in the SQL command
                #   Numbers -> no quotes around the value
                #   Strings -> quotes

                print("\tBuilding SQL command list")
                cmdList = buildSQL_insertDataFrame(hConn, cfg_db['value']['database'], db_tableName, dat)

                if cmdList[0] is not None:
                    # print(dispSQL(cmdList[0]))

                    print("\tSending SQL commands")

                    # print(dispSQL(cmdList[0]))

                    hCursor = hConn.cursor()
                    # hCursor.execute(cmdList[0])
                    # print(hCursor.fetchall())
                    # print(pandas.read_sql(cmdList[0], hConn))

                    for cmdStr in cmdList:

                        # print(dispSQL(cmdStr))

                        hCursor.execute(cmdStr)
                        res = hCursor.fetchall()

                        if len(res) > 0:
                            print(dispSQL(cmdStr))
                            print("\n\tResulting fetch:")
                            print("\t\tLength: "+ str(len(res)))
                            print(res)

                    print("\tNo more SQL commands to send")
                else:
                    print("\t\tError: First command is empty. Aborting")
        # Close connection
        hConn.close()


    return



def dispSQL(cmdStr: str) -> str:
    # Every X characters, insert "\n\t\t" to make the code more readable
    #   \n causes a new line
    #   \t causes one indent
    #       Take X characters, add a new line and 2 indents, then take another X characters, etc
    # Characters per line?
    perLine = 100
    # Total number of characters?
    lenStr = len(cmdStr)

    if lenStr <= perLine:
        # No need to add a new line
        dispStr = "\t\t" + cmdStr
    else:
        # Need to add at least 1 line
        #   How many lines do we need in total?
        nLine = math.ceil(lenStr/perLine)

        # Format very first line
        dispStr = "\t\t" + cmdStr[0:perLine]
        # Move on to next lines
        if nLine == 2:
            # Add only 1 new line
            dispStr = dispStr + "\n\t\t" + cmdStr[perLine:lenStr]
        else:
            # Have more than 1 new line
            for iLine in range(1,nLine-1):
                dispStr = dispStr + "\n\t\t" + cmdStr[perLine*iLine:perLine*(iLine+1)]
            dispStr = dispStr + "\n\t\t" + cmdStr[perLine*(nLine-1):lenStr]
            print(cmdStr[perLine*nLine:lenStr])
    return dispStr

def buildSQL_existDB(dbName: str) -> str:
    cmdStr = "SELECT schema_name FROM information_schema.schemata" + \
             " WHERE schema_name='" + dbName + "';"
    return cmdStr

def buildSQL_existTable(dbName: str, tableName: str) -> str:
    cmdStr = "SELECT table_name FROM information_schema.tables WHERE table_schema='" + \
             dbName + "' AND table_name='" + tableName + "';"
    return cmdStr

def buildSQL_createTable(dbName: str, tableName: str, dat: pandas.core.frame.DataFrame) -> str:
    # Get column headers
    colHeaders = list(dat)
    # Add primary key
    colHeaders = ["EntryID"] + colHeaders

    # Build string
    #   First column must be the primary key for this command to work
    tempStr = [None] * len(colHeaders)
    iCol = 0
    for colName in colHeaders:
        if iCol == 0:
            tempStr[iCol] = colName + " int NOT NULL AUTO_INCREMENT"
        else:
            colName_len = len(colName)
            colName_lower = colName.lower()
            if colName_len >= 6 and colName_lower[0:4] == 'date':
                if colName_len == 4:
                    # dataType = "DATE NOT NULL"
                    dataType = "DATE"
                else:
                    # dataType = "INT NOT NULL"
                    dataType = "INT(10)"
            elif colName_len == 3 and colName_lower == "day":
                # dataType = "CHAR(9) NOT NULL"
                dataType = "CHAR(9)"
            elif colName_len == 4 and colName_lower == "time":
                # dataType = "INT NOT NULL"
                dataType = "INT(10)"
            elif colName_len == 5 and colName_lower == "jobid":
                # dataType = "INT NOT NULL"
                dataType = "INT(10)"
            elif colName_len == 13 and (
                            colName_lower == "state changes" or colName_lower == "state_changes"):
                # dataType = "INT NOT NULL"
                dataType = "INT(10)"
            elif colName_len == 9 and (
                        colName_lower == "exit code" or colName_lower == "exit_code"):
                # dataType = "INT NOT NULL"
                dataType = "INT(10)"
            elif colName_len == 9 and (
                        colName_lower == "num nodes" or colName_lower == "num_nodes"):
                # dataType = "INT NOT NULL"
                dataType = "INT(10)"
            elif colName_len == 8 and (
                        colName_lower == "num cpus" or colName_lower == "num cpus"):
                # dataType = "INT NOT NULL"
                dataType = "INT(10)"
            elif colName_len == 8 and (
                        colName_lower == "job name" or colName_lower == "job name"):
                # dataType = "VARCHAR(100) NOT NULL"
                dataType = "VARCHAR(100)"
            else:
                dataType = "VARCHAR(25)"

            tempStr[iCol] = colName + " " + dataType

        iCol = iCol + 1

    # cmdStr = "CREATE TABLE " + dbName + "." + \
    #          db_tableName + " (" + ",".join(tempStr) + \
    #          ", PRIMARY KEY (" + colHeaders[0] + ")) ENGINE=InnoDB;"
    cmdStr = "CREATE TABLE " + dbName + "." + \
             db_tableName + " (" + ",".join(tempStr) + \
             ", PRIMARY KEY (" + colHeaders[0] + ")) ENGINE=MyISAM;"
    return cmdStr

def buildSQL_insertDataFrame(hConn: mysqlConnection,
                             dbName: str, tableName: str,
                             dat: pandas.core.frame.DataFrame) -> list:
    cmdList = [None] * dat.shape[0]

    # Get column headers
    colHeaders = list(dat)

    # Determine which columns need to have quotes around their values
    colTypeList = buildSQL_insertDataFrame__getColType(hConn, dbName, tableName, colHeaders)
    #   colTypeList is such that 0 means you do include quotes around the value, 1 you do not include quotes
    #   colTypeList[idx] = -1 means the column doesn't exist

    colTypeList = numpy.asarray(colTypeList)
    if any(colTypeList == -1):
        # Initialize index list
        idxList = numpy.asarray( range(0,len(colHeaders)) )
        # Identify indices to be dropped
        idxList = list( idxList[colTypeList==-1] )

        # Drop from
        #   dat
        #   colHeaders
        #   colTypeList
        #   cmdList
        dat = dat.drop(dat.columns[[idxList]],axis=1)

        colHeaders = numpy.delete(colHeaders, idxList).tolist()

        colTypeList = numpy.delete(colTypeList, idxList)

        cmdList = numpy.delete(cmdList, idxList).tolist()

    # Ready to build up a list of commands
    for iRow in range(0,dat.shape[0]):
    # for iRow in range(0,1):
        # Format is
        #       INSERT INTO [table] ([col1] , [col2] , ... ) VALUES ([val1] , [val2] , ... )

        # Build up "INSERT INTO [table] ("
        cmdList[iRow] = 'INSERT INTO ' + dbName + '.' + tableName + ' ('
        # Append "[col1] , [col2] , ..."
        for colName in colHeaders[0:-1]:
            cmdList[iRow] = cmdList[iRow] + colName + ','
        # Append "[col N]) VALUES ("
        cmdList[iRow] = cmdList[iRow] + colHeaders[-1] + ') VALUES ('

        # We now have
        #       INSERT INTO [table] ([col1] , [col2] , ... ) VALUES (
        #   Add "[val1] , [val2] , ... );"
        #       Remember, some values need quotes around them
        for iCol in range(0,dat.shape[1]-1):
            if colTypeList[iCol] == 0:
                cmdList[iRow] = cmdList[iRow] + '"' + dat.irow(iRow)[iCol] + '",'
            else:
                cmdList[iRow] = cmdList[iRow] + dat.irow(iRow)[iCol] + ','
        # Append ");"
        if colTypeList[-1] == 0:
            cmdList[iRow] = cmdList[iRow] + '"' + dat.irow(iRow)[-1] + '");'
        else:
            cmdList[iRow] = cmdList[iRow] + dat.irow(iRow)[-1] + ');'

    return cmdList

def buildSQL_insertDataFrame__getColType(hConn: mysqlConnection,
                                         dbName: str, tableName: str, colList: list
                                         ) -> list:
    # Will return a 0 for strings/dates and 1 for numerics
    #   When inserting into a column with type 0, you need to include quotes with the new value
    #                                          1, you do not include quotes around the value
    typeList = [None]*len(colList)

    cmdStr = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='" + \
             dbName + "' AND table_name='" + tableName + "';"
    res = pandas.read_sql(cmdStr, hConn)

    iCol = 0
    for colName in colList:
        temp = res[res['column_name'] == colName]['data_type']
        if temp.empty:
            typeList[iCol] = -1
        else:
            dataType = temp.values[0]
            dataType_len = len(dataType)

            if dataType_len >= 3 and (
                            dataType[dataType_len-3::].lower() == "int" or
                            dataType[0:3].lower() == "int"):
                typeList[iCol] = 1
            else:
                typeList[iCol] = 0

            iCol = iCol + 1

    return typeList



def login(hBrowser: webdriver.firefox.webdriver.WebDriver) -> bool:

    # Find login user name and password fields
    elem_user = None
    elem_pass = None
    elem_login = None

    #   Elements are grouped within id=loginForm
    loginForm = hBrowser.find_elements_by_id('loginForm')
    for iLoginForm in loginForm:
        # Get username and password from (xpath) //input
        elemList = iLoginForm.find_elements_by_xpath(".//input")
        for hElem in elemList:
            tempStr = hElem.get_attribute('name')

            if tempStr.find('user') > 0:
                elem_user = hElem
            elif tempStr.find('pass') > 0:
                elem_pass = hElem

            if elem_user is not None and elem_pass is not None:
                break

        # Get login button from id='startBut'
        LoginButton = iLoginForm.find_elements_by_id('startBut')
        for hElem in LoginButton:
            if hElem.get_attribute('onclick') is not None:
                tempStr = hElem.get_attribute('onclick')

                if tempStr.find('loginForm') > 0:
                    elem_login = hElem

                    break

        if elem_user is not None and elem_pass is not None and elem_login is not None:
            # Found username and password and login button
            break

    if elem_user is not None and elem_pass is not None and elem_login is not None:
        # Found username and password and login button
        # Enter login details
        cfg = pandas.read_csv(cfg_login_folderpath + cfg_login_filepath)

        elem_user.send_keys(cfg['value']['username'])
        elem_pass.send_keys(cfg['value']['password'])
        elem_login.click()

        return True
    else:
        return False



def login_wait(hBrowser: webdriver.firefox.webdriver.WebDriver,
               T: float, dT: float) -> bool:
    #   Wait for content to be loaded
    atView = False

    #       Step 1: Find content group
    for i in range(0, round(T / dT)):
        elemList = hBrowser.find_elements_by_id("content")
        if len(elemList) == 0:
            # Not loaded
            sleep(dT)
        else:

            # Found content group and it's not empty
            #   Make sure graphs have loaded
            #       They are under img with class "graphimage"
            for hElem in elemList:
                subList = hElem.find_elements_by_xpath('.//img[@class="graphimage"]')

                if len(subList) > 0:
                    atView = True
                    for hSub in subList:
                        if hSub.get_attribute('alt') is None:
                            atView = False
                            break

                    break
            if atView:
                break

    return atView



def logout(hBrowser: webdriver.firefox.webdriver.WebDriver) -> bool:
    print("Logging out...")
    sleep(0.5)
    elemList = hBrowser.find_elements_by_xpath("//*[contains(@href,'logout')]")

    if len(elemList) != 1:
        print("Error: Unable to find logout element")
        print("\tYou will need to logout manually")
        return False
    else:
        cURL = hBrowser.current_url
        iClick = 0
        atView = True
        while hBrowser.current_url == cURL:
            elemList[0].click()

            iClick = iClick + 1
            if iClick > 10:
                print("Aborting - Too many logout attempts")
                atView = False
                break
            else:
                sleep(0.5)

        return atView

def closeBrowser(hBrowser: webdriver.firefox.webdriver.WebDriver) -> bool:
    try:
        hBrowser.quit()
        return True
    except:
        return False



def gridView(hBrowser: webdriver.firefox.webdriver.WebDriver, dT_page: float) -> bool:
    # Find the Grid tab and press it
    #   Find element with link (href) to /cacti/plugins/grid/grid_summary

    #
    # Update to make more efficient
    #

    atView = False
    elemList = hBrowser.find_elements_by_xpath("//*[contains(@href,'grid_summary')]")
    for hElem in elemList:
        if hElem.get_attribute('href').find("/cacti/plugins/grid/"):
            atView = True

            # Found Grid tab
            #   Need to have a loop for clicking the tab because
            #   sometimes the page hasn't actually finished loading
            #       Put in a threshold to stop excessive clicks
            cURL = hBrowser.current_url
            iClick = 0
            while hBrowser.current_url == cURL:
                hElem.click()

                iClick = iClick + 1
                if iClick > 20:
                    print("Aborting - Too many click attempts")
                    atView = False
                    break
                else:
                    sleep(dT_page)

            break

    return atView


def jobdetailsView(hBrowser: webdriver.firefox.webdriver.WebDriver,
                   T: float, dT: float, dT_page: float) -> bool:
    atView = False
    # By clicking, we cause the page to refresh and need to re-gather the element references
    sleep(dT)
    for i in range(0, round(T / dT)):
        elemList = hBrowser.find_elements_by_id('grid_bjobs.php')
        if len(elemList) == 0:
            # Not loaded
            sleep(dT * 2)
        elif len(elemList) == 1:
            for hElem in elemList:
                atView = True

                cURL = hBrowser.current_url
                iClick = 0
                while hBrowser.current_url == cURL:
                    try:
                        hElem.click()

                        iClick = iClick + 1
                        if iClick > 10:
                            print("Aborting - Too many click attempts")
                            atView = False
                            break
                        else:
                            sleep(dT_page)
                    except WebDriver_Except.StaleElementReferenceException:
                        print("Error: Element ref is stale. Breaking While loop")
                        break

                break

            break
        else:
            break

    if atView:
        # Should verify that the page is done refreshing
        sleep(1)

        # Within Batch Job Filters, change STATUS from RUNNING to ACTIVE
        hBrowser.find_element_by_xpath("//select[@name='status']/option[text()='ACTIVE']").click()

        # Verify that page has content has been updated
        sleep(1)

    return atView



def identifyCactiTables(hBrowser: webdriver.firefox.webdriver.WebDriver):
    # Have 2 cacti tables
    #   1) Contains current data
    #   2) Contains coloring and last refresh
    # Find which one has "Last Refresh", which will be the non-data table
    table_data = None
    table_refresh = None

    elemList = hBrowser.find_elements_by_xpath('//table[@class="cactiTable"]')
    if len(elemList) == 2:
        for hElem in elemList:
            subList = hElem.find_elements_by_xpath(".//strong[text()='Last Refresh']")

            if len(subList) == 0:
                table_data = hElem
            else:
                table_refresh = hElem
                # Can double check with attribute innerHTML = Last Refresh
    else:
        print("Too many cacti tables found!")

    return table_data, table_refresh



def getNumJobs(table_data: webdriver.firefox.webelement.FirefoxWebElement):
    # How many rows do we have?
    #   Label is found at top and bottom of table under "noprint" then align="center"
    elemList = table_data.find_elements_by_xpath('.//*[@class="noprint"]')
    hElem = elemList[0]
    subList = hElem.find_elements_by_xpath('.//*[@align="center"]')
    if len(subList) != 1:
        print("Error finding # of results")
        return None
    else:
        # Only need total number of entries

        # resultRange = [None]*3
        # # innerHTML = subList[0].get_attribute('innerHTML')
        # # innerHTML = '\n\t\t\t\t\t\t\tShowing Rows 1 to X of XX [<strong><a class=" ...
        # # split by first '['
        # #       '\n\t\t\t\t\t\t\tShowing Rows 1 to X of XX '
        # # invert then split by first \t
        # #       ' XX fo X ot 1 swoR gniwohS'
        # #       '\t\t\t\t\t\t\n'
        # # get first entry
        # #   inverted for clarity
        # #       'Showing Rows 1 to X of XX '
        #
        # tempStr = subList[0].get_attribute('innerHTML').split("[",1)
        # tempStr = tempStr[0][::-1].split("\t",1)[0]
        #
        # # Get total first
        # tempStr = tempStr.split("f",1)
        # resultRange[2] = int( tempStr[0][::-1] )
        #
        # # Get end of current range
        # tempStr = tempStr[1][1::]
        # tempStr = tempStr.split("o",1)
        # resultRange[1] = int(tempStr[0][::-1])
        #
        # # Get start of current range
        # resultRange[0] = int( tempStr[1][1::].split("s",1)[0] )
        #
        # print(resultRange)


        # tempStr = subList[0].get_attribute('innerHTML').split("[", 1)
        # tempStr = tempStr[0][::-1].split("\t", 1)[0]
        # tempStr = tempStr.split("f", 1)
        # nRow = int(tempStr[0][::-1])

        return int(
            subList[0].get_attribute('innerHTML').split("[", 1)[0][::-1].split("\t", 1)[0].split("f", 1)[0][::-1] )



def jobsView_setRows(hBrowser: webdriver.firefox.webdriver.WebDriver, nRow: int) -> bool:
    # How many jobs may be displayed according to filter?
    tempStr = hBrowser.current_url.split("rows_selector=")
    tempStr2 = tempStr[1].split("&", 1)

    nFilter = int( tempStr2[0] )

    if nRow <= nFilter:
        # Don't need to update
        return True
    else:
        # Set new URL
        hBrowser.get(tempStr[0] + "rows_selector=" + str(nRow) + "&" + tempStr2[1])
        return False



def getHeaders(table_data: webdriver.firefox.webelement.FirefoxWebElement) -> (list, int):
    # Get columns
    #   class = 'tableSubHeader'
    hHeaderParent = table_data.find_element_by_xpath(
        './/*[@class="tableSubHeader"]')
    headerList = hHeaderParent.find_elements_by_xpath('*')

    nCol = len(headerList)
    colHeader = [None] * nCol

    iCol = 0
    for hHeader in headerList:
        # Find header element with
        #       find_element_by_xpath('.//a')
        # Get text with
        #       get_atribute('text')
        # Remove white spaces with
        #       replace(" ","_")
        colHeader[iCol] = hHeader.find_element_by_xpath('.//a').get_attribute(
            'text').replace(" ","_")
        iCol = iCol + 1

    return colHeader, nCol



def cheatsheet(nCol: int) -> (list, list):
    # Build cheatsheet for retrieving data
    cheatsheet_childLevels = [None] * nCol
    cheatsheet_attribute = copy.deepcopy(cheatsheet_childLevels)

    #   JobID
    iCol = 0
    cheatsheet_childLevels[iCol] = 1
    cheatsheet_attribute[iCol] = 'title'

    #   JobName
    iCol = 1
    cheatsheet_childLevels[iCol] = 0
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   Project Name
    iCol = 2
    cheatsheet_childLevels[iCol] = 1
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   Status
    iCol = 3
    cheatsheet_childLevels[iCol] = 0
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   State Changes
    iCol = 4
    cheatsheet_childLevels[iCol] = 0
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   Exit Code
    iCol = 5
    cheatsheet_childLevels[iCol] = 0
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   User ID
    iCol = 6
    cheatsheet_childLevels[iCol] = 1
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   Mem Request
    iCol = 7
    cheatsheet_childLevels[iCol] = 0
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   Mem Reserved
    iCol = 8
    cheatsheet_childLevels[iCol] = 0
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   Mem Wasted
    iCol = 9
    cheatsheet_childLevels[iCol] = 0
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   Max Memory
    iCol = 10
    cheatsheet_childLevels[iCol] = 0
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   CPU Usage
    iCol = 11
    cheatsheet_childLevels[iCol] = 0
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   CPU Effic
    iCol = 12
    cheatsheet_childLevels[iCol] = 0
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   Num Nodes
    iCol = 13
    cheatsheet_childLevels[iCol] = 0
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   Num CPUs
    iCol = 14
    cheatsheet_childLevels[iCol] = 0
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   Execution Host
    iCol = 15
    cheatsheet_childLevels[iCol] = 1
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   Running Queue
    iCol = 16
    cheatsheet_childLevels[iCol] = 1
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   Start Time
    iCol = 17
    cheatsheet_childLevels[iCol] = 0
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   End Time
    iCol = 18
    cheatsheet_childLevels[iCol] = 0
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   Pend
    iCol = 19
    cheatsheet_childLevels[iCol] = 0
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   Run
    iCol = 20
    cheatsheet_childLevels[iCol] = 0
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   SSusp
    iCol = 21
    cheatsheet_childLevels[iCol] = 0
    cheatsheet_attribute[iCol] = 'innerHTML'

    #   App
    iCol = 22
    cheatsheet_childLevels[iCol] = 1
    cheatsheet_attribute[iCol] = 'innerHTML'

    return cheatsheet_childLevels, cheatsheet_attribute



def getData(table_data: webdriver.firefox.webelement.FirefoxWebElement,
            colHeader: list, nCol: int, nRow: int):

    # Find parent element
    elemList = table_data.find_elements_by_xpath('.//*[@class="noprint"]')
    hElem = elemList[0]
    hParent = hElem.find_element_by_xpath('.//..')

    # Get list of children
    hChildList = hParent.find_elements_by_xpath('*')
    nChild = len(hChildList)
    #       Verify number - should be 3 more than number of data rows
    if nChild != 3 + nRow:
        print("\tError: Mismatch between number of results and number of elements")
        return None
    else:
        # Get list of children just related to data rows
        dataElem = [None] * nRow
        dataJobID = copy.deepcopy(dataElem)
        iRow = 0
        for hChild in hChildList:
            if hChild.get_attribute('id') is not None and len(
                    hChild.get_attribute('id')) != 0:
                dataElem[iRow] = hChild
                dataJobID[iRow] = hChild.find_element_by_xpath(
                    '*').find_element_by_xpath('*').get_attribute('title')
                iRow = iRow + 1

        if nRow != iRow:
            print("\tWarning: Mismatch between expected and actual number of Jobs!")
            print("\tNumber of entries expected = " + str(nRow))
            print("\tNumber of entries found = " + str(iRow))
            nRow = iRow

        # Initialize panda frame
        #   Get current date and time
        dateStr = strftime("%Y-%m-%d")
        dateStr2 = dateStr.split("-")
        timeStr = strftime("%H%M")
        dayStr = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        #   Adjust for timestamp
        nCol_adj = 6
        colHeader = ["Date", "Day", "Date_YYYY", "Date_MM", "Date_DD", "Time"] + colHeader
        nCol = nCol + nCol_adj

        #   Initialize
        data = pandas.DataFrame(None, index=[dataJobID[0], dataJobID[1]],
                                columns=colHeader)

        # Get cheatsheet for filling in data
        cheatsheet_childLevels, cheatsheet_attribute = cheatsheet(nCol-nCol_adj)

        # Fill data
        for iRow in range(0, nRow):
            hChildrenList = dataElem[iRow].find_elements_by_xpath('*')
            idx_row = dataJobID[iRow]
            for iCol in range(0, nCol):
                idx_col = colHeader[iCol]

                if iCol < nCol_adj:
                    if iCol == 0:
                        data.set_value(idx_row, idx_col, dateStr)
                    elif iCol == 1:
                        data.set_value(idx_row, idx_col,
                           dayStr[
                               datetime.date(
                                   int(dateStr2[0]), int(dateStr2[1]), int(dateStr2[2])
                               ).weekday()
                           ])
                    elif iCol == nCol_adj-1:
                        data.set_value(idx_row, idx_col, timeStr)
                    else:
                        data.set_value(idx_row, idx_col, dateStr2[iCol-2])
                else:
                    if cheatsheet_childLevels[iCol-nCol_adj] == 0:
                        data.set_value(idx_row, idx_col,
                                       hChildrenList[iCol-nCol_adj].get_attribute(cheatsheet_attribute[iCol-nCol_adj]))

                    elif cheatsheet_childLevels[iCol-nCol_adj] == 1:
                        data.set_value(idx_row, idx_col,
                                       hChildrenList[iCol-nCol_adj].find_element_by_xpath('*').get_attribute(
                                           cheatsheet_attribute[iCol-nCol_adj]))

        return data



def gridFileTransfer(source: str, dest: str) -> bool:
    # Load config
    # Set up SSH
    # Transfer file
    # Close SSh and file transfer

    cfg = pandas.read_csv(cfg_grid_folderpath + cfg_grid_filepath)

    hTransport = paramiko.Transport(( cfg['value']['host'], int( cfg['value']['port'] ) ))

    I_flag = False
    try:
        hTransport.connect(username=cfg['value']['username'], password=cfg['value']['password'])
        I_flag = True
    except:
        print("Error: Invalid host (address or port) or invalid login")
        print("\tHost=" + cfg['value']['host'] + ":" + cfg['value']['port'])
        print("\tUser=" + cfg['value']['username'])

    if I_flag:
        I_flag = False
        hSFTP = paramiko.SFTPClient.from_transport(hTransport)
        try:
            hSFTP.put(source, dest)
            I_flag = True
        except:
            print("Error: Unable to transfer source to destination")
            print("\tSource=" + source)
            print("\tDestination=" + dest)
        hSFTP.close()
    hTransport.close()

    return I_flag



def main():
    # Start browser
    print("Starting browser")
    print("\tGecko Driver at:")
    print("\t\t" + geckoDriver_path)
    print("\tURL:")
    print("\t\t" + url)
    hBrowser = webdriver.Firefox(executable_path=geckoDriver_path)
    hBrowser.get(url)

    # login
    print("Logging into PAC")
    atView = login(hBrowser)

    if not atView:
        print("\tError: Failed to login\n")
        print("Closing browser session")
        hBrowser.quit()
    else:
        print("\tLogin successful")

        # Define wait parameters
        dT = 0.1
        T = 5
        # dT_page = 0.25
        dT_page = 0.5

        # Wait for page to finish loading
        atView = login_wait(hBrowser, T, dT)

        if not atView:
            print("\tError: Failed to load page post-login\n")
            print("Closing browser session")
            hBrowser.quit()
        else:
            # Content objects loaded. Page should be loaded
            #   Find the Grid tab
            print("Changing View -> Grid")
            atView = gridView(hBrowser, dT_page)

            if not atView:
                print("\tError: Failed to switch to Grid view")
                print("Closing browser session")
                hBrowser.quit()
            else:
                # At Grid tab, now go to Job Info -> Details
                print("Changing View -> Grid Job Details")
                atView = jobdetailsView(hBrowser, T, dT, dT_page)

                if not atView:
                    print("\tError: Failed to switch to Grid Job Details view")
                    print("Closing browser session")
                    hBrowser.quit()
                else:
                    # Grab data
                    #   Identify how many rows of data we have available and in total
                    #       In order to grab the data, we need to know which cacti table is which
                    table_data, table_refresh = identifyCactiTables(hBrowser)

                    if table_data is None or table_refresh is None:
                        print("Aborting - Unable to identify data table")
                    else:
                        # Find out how many Jobs there are
                        nRow = getNumJobs(table_data)

                        if nRow is None:
                            print("Error: Unable to determine how many Job entries are available")
                        elif nRow == 0:
                            print("No Job entries available")
                        else:
                            # If we have more Jobs than may be displayed, we need to adjust filters
                            atView = jobsView_setRows(hBrowser, nRow)
                            if not atView:
                                # Page will load quickly
                                sleep(dT_page)

                                # Get new element references
                                table_data, table_refresh = identifyCactiTables(hBrowser)

                            if table_data is None or table_refresh is None:
                                print("Aborting - Unable to identify data table")
                            else:
                                # We are now at the right view and know how many Jobs there are
                                #   How many columns and what are the headers?
                                colHeader, nCol = getHeaders(table_data)

                                # Ready to now get the Jobs data
                                print("Grabbing data")
                                dat = getData(table_data, colHeader, nCol, nRow)
                                # print(dat)

                                atView = logout(hBrowser)
                                if atView:
                                    print("\tSuccess")
                                    print("\tClosing browser...")
                                    atView = closeBrowser(hBrowser)
                                    if atView:
                                        print("\t\tSuccess")

                                # Now want to store the data
                                #   Save the data to a file
                                #   Move file over to grid
                                #   Import into MariaDB

                                print("Storing data...")

                                # Saving to file
                                folderPath_local = "C:\\dump\\WebScrape_cacti\\"

                                #   Get date and time to build filename
                                fileName = "catciScrape" + strftime("_%Y_%m_%d__%H_%M") + ".csv"

                                # #   Save local copy
                                # dat.to_csv(folderPath_local + fileName)


                                # # Attempt to move file to grid
                                # folderPath_remote = "dump/WebScrape_cacti/"
                                # I_transfer = gridFileTransfer(
                                #     (folderPath_local+fileName),
                                #     (folderPath_remote+fileName))
                                #
                                # if I_transfer:
                                #     # Delete local copy
                                #     os.remove(folderPath_local + fileName)
                                #
                                #     # Import data into database
                                I_flag = dbInsert(dat)
                                print("Done")



if __name__ == '__main__':
    main()