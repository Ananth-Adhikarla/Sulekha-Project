import common
# import requests module
import requests
import random
import mythreads # custom class
from enum import Enum
import time
from signal import signal, SIGINT,SIGTSTP
import threading

class StatusFlags(Enum):
    free = 0
    using = 1
    finish = 2

class Flags(Enum):
    city = 0
    category = 1
    subcategory = 2
    business = 3

'''
Done: Getting subcategory data from table , getting all results for business listing for each subcategory - name + link
TO DO
Get contact info for each listing
Check Duplicates
Save data to table
'''

'''
List to store data
'''
subcategoryList = []
businessList = []
contactList = []

#global variable to check if program can exit or not
canExit = False
lock = threading.Lock()

# handler for signals received
def handler(signal_received, frame):

    # if canExit is true then stop program after updating database
    print('Closing program after thread')
    mythreads.StopThreads(canExit)

def CheckSignals():
    signal(SIGINT, handler) # to handle CTRL + C event
    signal(SIGTSTP, handler) # to handle CTRL + Z event

def GetBusinessData():

    global canExit

    ResetThreadStatus() # Temporary for testing only will remove

    '''
    Status Count checks how many records have status 0
    Status 0 = free and not processed , status 1 = being used , status 2 = finished
    everytime a status is updated to 1 or 2 status count decreases by 1
    '''
    # first time to check at start if no data is processed
    statusCount = mythreads.GetStatusCount(Flags.subcategory)
    canExit = True
    CheckSignals() # check for signal before loop starts

    while(not statusCount):
        '''
        if statusCount still has status of 0 then start threads
        takes the subcategory flag (table to get data from)
        GetSubcategory - function name
        None - no arguments in GetSubcategory
        add processed data to subcategoryList
        and also returns a boolean value of True ( which means thread has finished the job and can exit if signal captured )
        '''
        CheckSignals() # check for signal during loop
        s = mythreads.StartThreads(Flags.subcategory,GetSubcategory,None,subcategoryList)
        canExit = s[1]

     # parse the website and fetch the raw data
#    businessData = FetchBusinessData(subcategoryList)

     # take the result and make the business list for each subcategory
#    res = MakeBusinessList(businessData)

    # Testing only
#    for row in businessData:
#        print(row)

#---------------------------------------------------------------------------------------------------------------------------

# Get subcategory data from database - SubcategoryID, SubcategoryName from Subcategory and CityName from City by joining foreign keys
# on CityID

def GetSubcategory(myqueue):

    conn = common.OpenDb()
    cursor = conn.cursor()
    cursor.execute("SELECT t1.SubcategoryID, t1.SubcategoryName, t1.SubcategoryLink, t2.CityName FROM Subcategory t1  LEFT JOIN City t2 on (t2.CityID = t1.CityID) Where t1.Status=? ORDER BY RANDOM() LIMIT 1",(StatusFlags.free.value,))
    subcategoryInfo = cursor.fetchone()
    common.CloseDb(conn)

    mythreads.PostThreadStatus(StatusFlags.using.value,subcategoryInfo[0],Flags.subcategory) # Updates Thread status to 1 - being used

    myqueue.put([subcategoryInfo]) # The put adds item to a queue.

    print("Q  ",myqueue.get())

    mythreads.PostThreadStatus(StatusFlags.finish.value,subcategoryInfo[0],Flags.subcategory) # Updates Thread status to 2 - finished

    return subcategoryInfo

#---------------------------------------------------------------------------------------------------------------------------
def ResetThreadStatus(): # only for testing

	conn = common.OpenDb()
	cursor = conn.cursor()
	cursor.execute("Update Subcategory Set Status=0 ")
	conn.commit()
	common.CloseDb(conn)

#---------------------------------------------------------------------------------------------------------------------------
def FetchBusinessData(subcategory):

    for i in range(len(subcategory)):
        print("Fetching Data for:	",subcategory[i][1],subcategory[i][3])
        '''
        variables / format subcategory list - [(1, 'ATMs', '/atms/chennai', 'Chennai')]
        get the category ID which is different for each subcategory from the website
        '''
        subcategoryName = subcategory[i][1] # validation  done in subcategory file
        subcategoryUrl = common.GetBaseUrl()+subcategory[i][2] # validation done in subcategory
        cityName = subcategory[i][3]
        cID = GetPageCategoryID(subcategoryUrl); # get the id from the website directly
        pageNr = 1 # base page number

        '''
        get partialvalue which is different for each category and a unique number from the website - 64 digit long
        get the total number of pages for each subcategory listing
        combine the url with partial value and category id from website and subcategory name and cityname from database
        '''
        partialValue = GetPartialPageData(subcategoryUrl)
        totalPages = GetNumberOfPages(partialValue,cID,pageNr,subcategoryName,cityName)
        print(totalPages)
        for i in range(1,totalPages):
            url = CombineUrl(partialValue,cID,str(i),subcategoryName,cityName)
            businessData = ParseBusinessPage(url)
            result = MakeBusinessList(businessData)

    #return list of data
    return result

def GetPageCategoryID(url): # check for validity must be number only

	soup = common.GetWebpageContents(url)
	id = soup.find_all('input', attrs={'id' : 'hdnCategoryId'})
	categoryID = id[0].get('value')
	if(common.CheckNumberValidity(categoryID)):
		return categoryID
	else:
		return

def GetPartialPageData(url):

	soup = common.GetWebpageContents(url)
	partialPageData = soup.find_all('input', attrs={'id' : 'partialPageData'})
	partialValue = partialPageData[0].get('value')
	if(common.CheckNameValidity(partialValue)):
		return partialValue
	else:
		return

def CombineUrl(partialValue,cID,pageNr,subcategoryName,cityName):

	if(common.CheckSpaces(subcategoryName) is not False):
		subcategoryName = common.CheckSpaces(subcategoryName)

	#url = 'https://www.sulekha.com/mvc5/lazy/v1/Listing/get-business-list?PartialPageData='+partialValue+'&Category='+cID+'&Filter={}&PageNr=1&Sort=&getQuoteVisiblity=&aboutEnabled=&CategoryName='+subcategoryName+'&CityName='+cityName+'&IsAboutEnabled=True&fp=0&tp=0&fa=0&ta=0&au=&GroupCityId=0'
	subcategoryBase = '/mvc5/lazy/v1/Listing/get-business-list?PartialPageData='+partialValue
	filters = '&Category='+cID+'&Filter={}&PageNr='+pageNr+'&Sort=&getQuoteVisiblity=&aboutEnabled=&CategoryName='+subcategoryName+'&CityName='+cityName
	remainder = '&IsAboutEnabled=True&fp=0&tp=0&fa=0&ta=0&au=&GroupCityId=0'

	url = subcategoryBase + filters + remainder

	if(common.CheckUrlValidity(url)):
		return common.GetBaseUrl()+url
	else:
		return

def GetHasMoreResults(url):

	soup = common.GetWebpageContents(url)
	hasMoreResults = soup.find_all('input', attrs={'id' : 'hdnBizHasMoreResults'})
	result = hasMoreResults[0].get('value')
	if(result == 'True'):
		return True
	else:
		return False


def GetNumberOfPages(partialValue,cID,pageNr,subcategoryName,cityName):

	# checks from 1st page till last page
	while True:
		hasMoreResults = GetHasMoreResults(CombineUrl(partialValue,cID,str(pageNr),subcategoryName,cityName))
		pageNr = int(pageNr) + 1
		if(hasMoreResults is False):
			break

	return pageNr

def ParseBusinessPage(url):

	soup = common.GetWebpageContents(url)
	businessHeader = soup.find_all('li', attrs={'class' : 'list-item view-r'})
	return soup

#---------------------------------------------------------------------------------------------------------------------------

def MakeBusinessList(businessData):

    for data in businessData:

        businessAnchorTag = data.find_all('a',attrs={'class' : 'bizlinkurl GAQ_C_BUSL busi-name'}) # anchor tag
        #print(businessAnchorTag)
        if(not businessAnchorTag):
            for row in businessAnchorTag:
                businessName = row.find('h3').string
                businessUrl = row['href']

                if(common.CheckNameValidity(businessName) and common.CheckUrlValidity(businessUrl)):
                    businessList.append([businessName,businessUrl])
                else:
                    return

    return businessList

#---------------------------------------------------------------------------------------------------------------------------


GetBusinessData()
