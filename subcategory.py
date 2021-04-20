import common
import requests
from enum import Enum
from signal import signal, SIGINT,SIGTSTP
import queue
import threading
import sys
import time
import re

class StatusFlags(Enum):
    free = 0
    using = 1
    finish = 2


'''
using queue will update 10 random records fetched from database and set status = 1 ( using )
finish queue will update the above queue to status 2 (finished)
'''
usingQueue = queue.Queue()
finishQueue = queue.Queue()

'''
List to store subcategories in
'''
categoryList = []
cityList = []
subcategoryList = []

'''
global variable to check if program can exit or not
'''
canExit = False
NUMBER_OF_RECORDS_TO_FETCH = 1

#---------------------------------------------------------------------------------------------------------------------------
'''
handler for signals received
'''
def handler(signal_received, frame):
    global canExit
    # if canExit is true then stop program after updating database
    print('Closing program after updating database')
    canExit = True
#---------------------------------------------------------------------------------------------------------------------------

'''
if signal is received to quit program - wait till loop is over then quit
In each loop :
Get Data ( URL ) from subcategory table
update subcategory record to status 1
Go to the url and get all business listings for all pages
verify and save to businessinfo table
update subcategory record to status 2
go to next record or stop program
'''

def StopProgram():
    global canExit
    if(canExit):
        sys.exit(0)

#---------------------------------------------------------------------------------------------------------------------------

def CheckSignals():
    '''
    to handle CTRL + C event
    '''
    signal(SIGINT, handler)

    '''
    to handle CTRL + Z event
    '''
    signal(SIGTSTP, handler)


#---------------------------------------------------------------------------------------------------------------------------
'''
 Will create threads based on the number of records to fetch from subcategory table
'''
def FetchCityRecords():

    fetch = [threading.Thread(target=CityQuery) for i in range(NUMBER_OF_RECORDS_TO_FETCH)]

    [t.start() for t in fetch]

    [t.join() for t in fetch]

#---------------------------------------------------------------------------------------------------------------------------
'''
CheckStatus - will count number of records in subcategory table where status is 0.
if CheckStatus returns more than 0 - then loop will continue to get records and process them or wait for exit signal
if CheckStaus returns 0 it will stop the loop
'''
def CheckStatus():

    conn = common.OpenDb()
    cursor = conn.cursor()
    cursor.execute("Select COUNT(Status) from City WHERE Status = 0")
    statusCount = cursor.fetchone()
    common.CloseDb(conn)
    return statusCount

#---------------------------------------------------------------------------------------------------------------------------

def GetSubcategoryData():

    ResetThreadStatus()
    threadStatus = CheckStatus()

    '''
    check for signal before loop starts
    '''
    CheckSignals()
    StopProgram()
    count = 1
    while(threadStatus[0] != 0):#threadStatus != 0

        '''
        check for signal during loop
        '''
        CheckSignals()
        StopProgram()

        '''
        threadStatus calls the function CheckStatus()
        CheckStatus gets the number of records with status 0 left
        if status is 0 it means that records have not been used
        if the threadStatus is 0 then while loop ends otherwise it keeps looping till it reaches 0
        '''
        StartProcessing()
        threadStatus = CheckStatus()

    #SubcategoryData(cityList,categoryList) # filter the data and check before saving

#---------------------------------------------------------------------------------------------------------------------------
def StartProcessing():

    # since data is not taken out of database still

    '''
    MyThreads creates threads and calls SubcategoryQuery function to fetch records from database
    '''
    FetchCityRecords()

    '''
    UpdateUsingQuery updates the 10 records above to status 1 - being used
    '''
    UpdateUsingQuery()

    '''
    get category data based on each city and add to list
    '''
    CategoryQuery()
    '''
    parse the website and fetch the raw data
    '''
    subcategoryData = FetchSubcategoryData(cityList,categoryList)

    SaveSubcategory()

    '''
    UpdateFinishQuery updates the 10 records above to status 2 - finished
    '''
    UpdateFinishQuery()

    '''
    Empty the list before going into next loop
    '''
    cityList.clear()
    categoryList.clear()
    subcategoryList.clear()

def CategoryQuery():

    #city = GetCity() # will be for all cities for now using only 1 - to remove parameters
    for cityRow in cityList:
        categoryData = ParseCityPage(common.GetBaseUrl()+cityRow[2])
        categoryList.append(categoryData)

def CityQuery():

    conn = common.OpenDb()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM City WHERE STATUS=? ORDER BY RANDOM() LIMIT 1",(StatusFlags.free.value,)) # select 1 random city
    cityInfo = cursor.fetchone()
    common.CloseDb(conn)

    '''
    put this data into list
    '''
    cityList.append(cityInfo)

    '''
    put this data into usingQueue - to update to status 1
    '''
    usingQueue.put([cityInfo])

    return cityInfo

#---------------------------------------------------------------------------------------------------------------------------
'''
set city status to 1 after the city records have been fetched and before processing
to block this row from being used again
'''
def UpdateUsingQuery():

    while not usingQueue.empty():
        info = usingQueue.get()
        conn = common.OpenDb()
        cursor = conn.cursor()
        cityID = info[0]

        cursor.execute("Update City Set Status=? Where CityID=?",(str(StatusFlags.using.value),str(cityID[0])))
        conn.commit()

        '''
        put the same record into finishQueue - to update to status 2
        '''
        finishQueue.put([cityID])
        common.CloseDb(conn)

#---------------------------------------------------------------------------------------------------------------------------
'''
set city status to 2 after the city records have been processed
'''
def UpdateFinishQuery():

    while not finishQueue.empty():
        info = finishQueue.get()
        print("FINISHING ")
        conn = common.OpenDb()
        cursor = conn.cursor()
        cityID = info[0]
        cursor.execute("Update City Set Status=? Where CityID=?",(str(StatusFlags.finish.value),str(cityID[0])))
        conn.commit()
        common.CloseDb(conn)


#---------------------------------------------------------------------------------------------------------------------------
def ResetThreadStatus(): # only for testing

    conn = common.OpenDb()
    cursor = conn.cursor()
    cursor.execute("Update City Set Status=0 ")
    conn.commit()
    common.CloseDb(conn)

#---------------------------------------------------------------------------------------------------------------------------
'''
get category id from database
'''
def GetCategoryID(categoryName):
    conn = common.OpenDb()
    cursor = conn.cursor()
    # get category id from table based on name given
    cursor.execute("Select CategoryID FROM Category WHERE CategoryName=?", [categoryName])
    categoryID = cursor.fetchone()
    return categoryID[0] # category ID (integer) is primary key taken from database

#---------------------------------------------------------------------------------------------------------------------------

def ParseCityPage(url):

	soup = common.GetWebpageContents(url)
	subcategoryheader = soup.find_all('div', attrs={'class' : 't-list'})
	return subcategoryheader

#---------------------------------------------------------------------------------------------------------------------------
'''
for each row in city ex: Chennai , bangalore
iterate through each category
extract subcategory names
add subcategory names to list
which is later used for adding to database in batches
'''
def FetchSubcategoryData(cityList,categoryList):

    for i in range(len(cityList)):

        for j in range(len(categoryList)):
            for category in categoryList[j]:
                if(category.find('h2') is not None): # only get records which is not empty / null
                    categoryHeader = category.find('h2').string
                    categoryID = GetCategoryID(categoryHeader)
                    subcategoryHeader = category.find('ul')
                    cityID = cityList[i][0]
                    cityName = cityList[i][1]

                    for subcategory in subcategoryHeader:
                        subName = subcategory.string
                        subUrl = subcategory.a['href']
                        print(subcategory)
                        if(CheckConditions(cityName,subName,subUrl)):
                            subcategoryList.append([categoryID,cityID,subName,subUrl])
                        else:
                            print('subcategory already exists',subName,subUrl)
                            return

#---------------------------------------------------------------------------------------------------------------------------

def CheckValidity(subcategoryName,subcategoryUrl):

	isValidSubcategoryName = common.CheckNameValidity(subcategoryName)
	isValidUrl = common.CheckUrlValidity(subcategoryUrl)
	if(isValidSubcategoryName and isValidUrl):
		return True
	else:
		return False

#---------------------------------------------------------------------------------------------------------------------------
'''
check if subcategory name and link is there in database
'''
def CheckDuplicates(subcategoryName, subcategoryLink):

    query = ("SELECT SubcategoryName,SubcategoryLink FROM Subcategory WHERE SubcategoryName=? AND SubcategoryLink=? ")
    result = common.CheckDuplicates(query,(subcategoryName, subcategoryLink))
    return result

#---------------------------------------------------------------------------------------------------------------------------
'''
Check Conditions will check for duplicates and validity
'''
def CheckConditions(cityName,subcategoryName, subcategoryLink):

    isValid = CheckValidity(subcategoryName,subcategoryLink)
    isDuplicate = CheckDuplicates(subcategoryName,subcategoryLink)

    '''
    reason for doing this is because 1 of the city is not function properly
    /credit-cards-agents/chennai this is what the link should look like
    but for city Bhubhaneswar its coming as default page - it shows results for all cities
    '''
    strSplit = subcategoryLink.split('/')
    cityCheck = strSplit[2]
    # taken from DB so first letter is capital and url has no caps
    cityName = cityName.lower()

    if(isValid is True and isDuplicate is False):
        if(cityName == cityCheck):
            return True
    else:
        return False

#---------------------------------------------------------------------------------------------------------------------------
'''
def SaveSubcategory(categoryID, cityID, subcategoryName, subcategoryLink):

	query = ('insert into Subcategory (CategoryID, CityID, SubcategoryName, SubcategoryLink) values (?,?,?,?)')
	result = common.SaveToDatabase(query,( categoryID, cityID, subcategoryName, subcategoryLink ))
	return result
'''
#---------------------------------------------------------------------------------------------------------------------------

def GetTime():
    '''
    time.time will return data in seconds
    1 second = 1000 milliseconds
    '''
    milli_sec = int(round(time.time() * 1000))
    return milli_sec

def SaveSubcategory():

    query = ('insert into Subcategory (CategoryID, CityID, SubcategoryName, SubcategoryLink) values (?,?,?,?)')
    start = GetTime()
    conn = common.OpenDb()
    cursor = conn.cursor()
    '''
    executemany will insert multiple rows at once same as transaction for batch processing
    in time comparison its much faster than line by line insert take on 5-10 ms for all business listing in each subcategory
    if number of listings is high (example: 17 pages of business listing) will take average of 15-30 ms
    testing on 4 terminals actively running
    '''
    cursor.executemany(query,subcategoryList)
    conn.commit()
    common.CloseDb(conn)

    finish = GetTime()
    print("TOTAL TIME IN MS ",finish-start)

    print("\n")
#---------------------------------------------------------------------------------------------------------------------------
'''
only used for testing to delete data from database - much faster than doing it from GUI (Sqlite DB Browser) if more than 1000 rows
'''

def DeleteRows():

    query = ("DELETE FROM Subcategory")

    conn = common.OpenDb()
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    common.CloseDb(conn)

GetSubcategoryData()
#DeleteRows()
