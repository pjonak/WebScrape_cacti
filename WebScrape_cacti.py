# Will be performing a screen scrape
#   Has login requirement therefore use Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By as WebDriver_By
from selenium.webdriver.support.ui import WebDriverWait as WebDriver_Wait
from selenium.webdriver.support import expected_conditions as WebDriver_ExpCond
from selenium.common import exceptions as WebDriver_Except
from time import sleep
from time import time
import copy
import pandas

#       Need geckodriver
geckoDriver_path = "C:\Python\Libraries\geckodriver\geckodriver_64bit.exe"

# Define website URL
url = 'https://researchpac.hbs.edu/cacti/'



# # Load browser
# hBrowser = webdriver.Firefox(executable_path=geckoDriver_path)
# # Go to website
# hBrowser.get(url)
#
# # Do something
# hBrowser.maximize_window()



hBrowser = webdriver.Firefox(executable_path=geckoDriver_path)


# Go to address
hBrowser.get(url)

# Find login user name and password fields
elem_user = None
elem_pass = None
elem_login = None

#   Grouped within id=loginForm
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
    cfg = pandas.read_csv("C:\\Users\\pjonak\\Documents\\keys\\pacConfig.txt")

    elem_user.send_keys(cfg['value']['username'])
    elem_pass.send_keys(cfg['value']['password'])
    elem_login.click()

    # Change view to:
    #     main tab, Grid
    #     left pane, Job Info -> Details
    atView = False

    # Wait for page to finish loading
    dT = 0.05
    T = 5
    dT_page = 0.25
    # for i in range(0,round(T/dT)):
    #     maintable = hBrowser.find_elements_by_xpath("//*[contains(@href,'grid_summary')]")
    #     if len(maintable) == 0:
    #         # Not loaded
    #         sleep(dT)
    #     else:
    #
    #         # Should verify that the page is done refreshing
    #         sleep(2)
    #
    #         break
    # for iMain in maintable:
    #     if iMain.get_attribute('href').find("/cacti/plugins/grid/"):
    #         atView = True
    #         iMain.click()
    #         break
    #
    # # Selected main tab, Grid?
    # if atView:
    #     atView = False
    #     # By clicking, we cause the page to refresh and need to re-gather the element references
    #     sleep(dT)
    #     for i in range(0, round(T / dT)):
    #         leftpane = hBrowser.find_elements_by_id('grid_bjobs.php')
    #         if len(leftpane) == 0:
    #             # Not loaded
    #             sleep(dT)
    #         elif len(leftpane) == 1:
    #
    #             # Figure out better way than "sleep" to be ready for mouse click
    #             sleep(2)
    #
    #             for iPane in leftpane:
    #                 atView = True
    #                 iPane.click()
    #             break
    #         else:
    #             break
    #
    # # Selected left pane, Job Info -> Details?
    # if atView:
    #     atView = False
    #
    #     # Should verify that the page is done refreshing
    #     sleep(2)


    # Wait for page to finish loading
    #   Wait for content to be loaded
    #       Step 1: Find content group
    atView = False
    for i in range(0,round(T/dT)):
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

    # Content objects loaded. Page should be loaded
    #   Find the Grid tab
    #       Find element with link (href) to /cacti/plugins/grid/grid_summary
    if atView:
        atView = False

        #
        # Update to make more efficient
        #
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

                    iClick = iClick+1
                    if iClick > 20:
                        print("Aborting - Too many click attempts")
                        atView = False
                        break
                    else:
                        sleep(dT_page)

                break

    # At Grid tab, now go to Job Info -> Details
    if atView:
        atView = False
        # By clicking, we cause the page to refresh and need to re-gather the element references
        sleep(dT)
        for i in range(0, round(T / dT)):
            elemList = hBrowser.find_elements_by_id('grid_bjobs.php')
            if len(elemList) == 0:
                # Not loaded
                sleep(dT*2)
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
                            print("Element ref is stale")
                            break

                    break

                break
            else:
                break

                # Selected left pane, Job Info -> Details?
    if atView:
        atView = False

        # Should verify that the page is done refreshing
        sleep(1)


        # Within Batch Job Filters, change STATUS from RUNNING to ACTIVE
        hBrowser.find_element_by_xpath("//select[@name='status']/option[text()='ACTIVE']").click()

        # Verify that page has content has been updated
        sleep(1)

        # Grab data
        #   Identify how many rows of data we have available and in total
        #       Have 2 cacti tables
        #           1) Contains current data
        #           2) Contains coloring and last refresh
        #       Find which one has "Last Refresh", which will be the non-data table
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

        if table_data is None or table_refresh is None:
            print("Aborting - Unable to identify data table")
        else:
            # How many rows do we have?
            #   Label is found at top and bottom of table under "noprint" then align="center"
            elemList = table_data.find_elements_by_xpath('.//*[@class="noprint"]')
            hElem = elemList[0]
            subList = hElem.find_elements_by_xpath('.//*[@align="center"]')
            if len(subList) != 1:
                print("Error finding # of results")
            else:
                resultRange = [None]*3
                # innerHTML = subList[0].get_attribute('innerHTML')
                # innerHTML = '\n\t\t\t\t\t\t\tShowing Rows 1 to X of XX [<strong><a class=" ...
                # split by first '['
                #       '\n\t\t\t\t\t\t\tShowing Rows 1 to X of XX '
                # invert then split by first \t
                #       ' XX fo X ot 1 swoR gniwohS'
                #       '\t\t\t\t\t\t\n'
                # get first entry
                #   inverted for clarity
                #       'Showing Rows 1 to X of XX '

                tempStr = subList[0].get_attribute('innerHTML').split("[",1)
                tempStr = tempStr[0][::-1].split("\t",1)[0]

                # Get total first
                tempStr = tempStr.split("f",1)
                resultRange[2] = int( tempStr[0][::-1] )

                # Get end of current range
                tempStr = tempStr[1][1::]
                tempStr = tempStr.split("o",1)
                resultRange[1] = int(tempStr[0][::-1])

                # Get start of current range
                resultRange[0] = int( tempStr[1][1::].split("s",1)[0] )

                print(resultRange)

                # Can we see all the data?
                # How many rows are there?
                #
                #   Get parent of elemList which pointed to class "noprint"
                #       hElem
                hParent = hElem.find_element_by_xpath('.//..')

                #   Get list of children
                hChildList = hParent.find_elements_by_xpath('*')
                nChild = len(hChildList)
                #       Verify number - should be 3 more than number of data rows
                if nChild != 3+(resultRange[1]-resultRange[0]+1):
                    print("Error: Mismatch between number of results and number of elements")

                else:
                    # # Get list of children just related to data rows
                    # dataElem = [None]*(resultRange[1]-resultRange[0]+1)
                    # dataJobID = copy.deepcopy(dataElem)
                    # dataURL = copy.deepcopy(dataElem)
                    #
                    # iRow = 0
                    # for hChild in hChildList:
                    #     if hChild.get_attribute('id') is not None and len(hChild.get_attribute('id')) != 0:
                    #         dataElem[iRow] = hChild.find_element_by_xpath('*').find_element_by_xpath('*')
                    #         dataJobID[iRow] = dataElem[iRow].get_attribute('title')
                    #         dataURL[iRow] = dataElem[iRow].get_attribute('href')
                    #         iRow = iRow+1
                    #
                    # # Ready to go through each URL and retrieve data
                    # #   Go to URL, get STATUS, then run appropriate parser
                    # cURL = hBrowser.current_url
                    #
                    # iRow = 0
                    #
                    # # hBrowser.get(dataURL[iRow])


                    # Get columns
                    #   class = 'tableSubHeader'
                    hHeaderParent = table_data.find_element_by_xpath('.//*[@class="tableSubHeader"]')
                    hHeaderList = hHeaderParent.find_elements_by_xpath('*')

                    nCol = len(hHeaderList)
                    colHeader = [None]*nCol

                    iCol = 0
                    for hHeader in hHeaderList:
                        colHeader[iCol] = hHeader.find_element_by_xpath('.//a').get_attribute('text')
                        iCol = iCol+1

                    # Get list of children just related to data rows
                    dataElem = [None] * (resultRange[1] - resultRange[0] + 1)
                    dataJobID = copy.deepcopy(dataElem)
                    iRow = 0
                    for hChild in hChildList:
                        if hChild.get_attribute('id') is not None and len(hChild.get_attribute('id')) != 0:
                            dataElem[iRow] = hChild
                            dataJobID[iRow] = hChild.find_element_by_xpath('*').find_element_by_xpath('*').get_attribute('title')
                            iRow = iRow + 1
                    nRow = iRow

                    # Initialize panda frame
                    data = pandas.DataFrame(None, index=[dataJobID[0],dataJobID[1]] ,columns=colHeader)

                    # Build cheatsheet for retrieving data
                    cheatsheet_childLevels = [None]*nCol
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

                    # Fill data
                    for iRow in range(0,nRow):
                        hChildrenList = dataElem[iRow].find_elements_by_xpath('*')
                        idx_row = dataJobID[iRow]
                        for iCol in range(0,iCol+1):
                            idx_col = colHeader[iCol]

                            if cheatsheet_childLevels[iCol] == 0:
                                data.set_value(idx_row, idx_col,
                                               hChildrenList[iCol].get_attribute(cheatsheet_attribute[iCol]) )

                            elif cheatsheet_childLevels[iCol] == 1:
                                data.set_value(idx_row, idx_col,
                                               hChildrenList[iCol].find_element_by_xpath('*').get_attribute(
                                                   cheatsheet_attribute[iCol] ) )

                    print(data)
                    # output to file for now
                    data.to_csv("C:\\Users\\pjonak\\Desktop\\cactiScrape.csv")








print("sleeping")
sleep(5)
print("done")
# hBrowser.quit()