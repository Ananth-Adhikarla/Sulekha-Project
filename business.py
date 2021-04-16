'''
Done: Getting subcategory data from table , getting all results for business listing for each subcategory - name + link
TO DO
Get contact info for each listing - new script
Check Duplicates - done
Save data to table - done
'''

import common
import requests
from enum import Enum
from signal import signal, SIGINT,SIGTSTP
import queue
import threading
import sys
import urllib.parse

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
List to store data
'''
subcategoryList = []
businessList = []
contactList = []

'''
global variable to check if program can exit or not
'''
canExit = False
NUMBER_OF_RECORDS_TO_FETCH = 2

def GetBusinessData():

    global canExit

    # Temporary for testing only will remove
    ResetThreadStatus()

    threadStatus = CheckStatus()

    '''
    check for signal before loop starts
    '''
    CheckSignals()
    StopProgram()
    count = 1
    while(threadStatus != 0):#threadStatus != 0

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

    #Testing only
#    print("OVER")
#    for row in businessList:
#        print(row)

#---------------------------------------------------------------------------------------------------------------------------
def StartProcessing():

    # since data is not taken out of database still

    '''
    MyThreads creates threads and calls SubcategoryQuery function to fetch records from database
    '''
    FetchSubcategoryRecords()
    print("SUBCATEGORY LIST ", subcategoryList)
    '''
    UpdateUsingQuery updates the 10 records above to status 1 - being used
    '''
    UpdateUsingQuery()

    '''
    parse the website and fetch the raw data
    '''
    businessData = FetchBusinessData(subcategoryList)

    '''
    UpdateFinishQuery updates the 10 records above to status 2 - finished
    '''
    UpdateFinishQuery()

    '''
    Empty the list before going into next loop
    '''
    subcategoryList.clear()


#---------------------------------------------------------------------------------------------------------------------------
'''
handler for signals received
'''
def handler(signal_received, frame):
    global canExit
    # if canExit is true then stop program after updating database
    print('Closing program after updating database')
    canExit = True

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
def FetchSubcategoryRecords():

    fetch = [threading.Thread(target=SubcategoryQuery) for i in range(NUMBER_OF_RECORDS_TO_FETCH)]

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
    cursor.execute("Select COUNT(Status) from Subcategory WHERE Status = 0")
    statusCount = cursor.fetchone()
    common.CloseDb(conn)
    return statusCount

#---------------------------------------------------------------------------------------------------------------------------

'''
Get 1 random record - Subcategory ID,Name,Link from subcategory and CityName by joining the CityID(needed for url) on City table
'''
def SubcategoryQuery():

    conn = common.OpenDb()
    cursor = conn.cursor()

    cursor.execute("SELECT SubcategoryID, SubcategoryName, SubcategoryLink FROM Subcategory Where Status=? ORDER BY RANDOM() LIMIT 1",(StatusFlags.free.value,))

    subcategoryInfo = cursor.fetchone()
    common.CloseDb(conn)

    '''
    put this data into list
    '''
    subcategoryList.append(subcategoryInfo)

    '''
    put this data into usingQueue - to update to status 1
    '''
    usingQueue.put([subcategoryInfo])

    return subcategoryInfo

#---------------------------------------------------------------------------------------------------------------------------
#todo: try without queues and check how it works
def UpdateUsingQuery():

    while not usingQueue.empty():
        info = usingQueue.get()
        conn = common.OpenDb()
        cursor = conn.cursor()
        subcategoryID = info[0]

        cursor.execute("Update Subcategory Set Status=? Where SubcategoryID=?",(str(StatusFlags.using.value),str(subcategoryID[0])))
        conn.commit()

        '''
        put the same record into finishQueue - to update to status 2
        '''
        finishQueue.put([subcategoryID])
        common.CloseDb(conn)

#---------------------------------------------------------------------------------------------------------------------------

def UpdateFinishQuery():

    while not finishQueue.empty():
        info = finishQueue.get()
        conn = common.OpenDb()
        cursor = conn.cursor()
        subcategoryID = info[0]
        cursor.execute("Update Subcategory Set Status=? Where SubcategoryID=?",(str(StatusFlags.finish.value),str(subcategoryID[0])))
        conn.commit()
        common.CloseDb(conn)

#---------------------------------------------------------------------------------------------------------------------------
# only for testing
def ResetThreadStatus():

	conn = common.OpenDb()
	cursor = conn.cursor()
	cursor.execute("Update Subcategory Set Status=0 ")
	conn.commit()
	common.CloseDb(conn)

#---------------------------------------------------------------------------------------------------------------------------

def FetchBusinessData(subcategory):

    for i in range(len(subcategory)):

        '''
        variables / format subcategory list - [(1, 'ATMs', '/atms/chennai')]
        validation done in subcategory
        '''
        subcategoryName = subcategory[i][1]
        subcategoryUrl = common.GetBaseUrl()+subcategory[i][2]
        subcategoryID = subcategory[i][0]
        print("\n")
        print("Fetching Data for: ",subcategoryName + "     Link: ",subcategoryUrl)
        print("\n")


        '''https://www.sulekha.com/direct-connection/allahabad is 404 - not working but is a valid url'''

        if(CheckResponseStatus(subcategoryUrl)):
            '''
            check if the page is of type listing or grandparent
            grandparent is subcategory inside a subcategory - has no business listing
            '''
            if(CheckPageType(subcategoryUrl)):
                '''
                example Delhi NCR is being used as Delhi - so better to take CityName from website instead of database
                '''
                cityName = GetCityName(subcategoryUrl)

                '''
                get the category ID which is different for each subcategory from the website and different from database
                used for making the url to parse data from website
                '''
                cID = GetPageCategoryID(subcategoryUrl);

                '''base page number'''
                pageNr = 1

                '''
                get partialvalue which is different for each category and a unique number from the website - 64 digit long
                get the total number of pages for each subcategory listing
                combine the url with partial value and category id from website and subcategory name and cityname from database
                '''
                partialValue = GetPartialPageData(subcategoryUrl)
                totalPages = GetNumberOfPages(partialValue,cID,pageNr,subcategoryName,cityName)
                print("Number of total pages : ",totalPages)
                PageThreads(totalPages,partialValue,cID,subcategoryName,cityName,subcategoryID)

#---------------------------------------------------------------------------------------------------------------------------
'''
using the number of totalpages - create a thread for each page and each thread will extract data from that page
'''
def PageThreads(totalPages,partialValue,cID,subcategoryName,cityName,subcategoryID):

    fetch = [threading.Thread(target=GetPageData ,args=(partialValue,cID,i,subcategoryName,cityName,subcategoryID)) for i in range(totalPages)]

    [t.start() for t in fetch]

    [t.join() for t in fetch]

#---------------------------------------------------------------------------------------------------------------------------
'''
used by the threads aboe to get the data from website and create a business list
'''
def GetPageData(partialValue,cID,i,subcategoryName,cityName,subcategoryID):

    url = CombineUrl(partialValue,cID,str(i),subcategoryName,cityName)
    businessData = ParseBusinessPage(url)
    result = ProcessDataToSave(businessData,subcategoryID)
    return result

#---------------------------------------------------------------------------------------------------------------------------
'''
checks if the given url returns status code of 200 = website is working / functional
'''
def CheckResponseStatus(url):

    response= requests.get(url)
    status= response.status_code
    if(status == 200):
        return True
    else:
        return False

#---------------------------------------------------------------------------------------------------------------------------
'''
get the category id
check for validity must be number only
<input type="hidden" id="hdnCategoryId" value="303">
'''
def GetPageCategoryID(url):

	soup = common.GetWebpageContents(url)
	id = soup.find_all('input', attrs={'id' : 'hdnCategoryId'})
	categoryID = id[0].get('value')
	if(common.CheckNumberValidity(categoryID)):
		return categoryID
	else:
		return

#---------------------------------------------------------------------------------------------------------------------------
'''
check type of page if grandparent or listing page
<input type="hidden" id="hdnpageviewtype" value="Listing page">
<input type="hidden" id="hdnpageviewtype" value="GrandParent">
'''
def CheckPageType(url):

    soup = common.GetWebpageContents(url)
    pageType = soup.find_all('input', attrs={'id' : 'hdnpageviewtype'})

    if(pageType[0].get('value') == 'GrandParent'):
        return False
    elif(pageType[0].get('value') == 'Listing page'):
        return True
    else:
        return

#---------------------------------------------------------------------------------------------------------------------------
'''
get the name of the city for the url given
<input type="hidden" id="hdnCityName" value="Chennai">
'''
def GetCityName(url):

    soup = common.GetWebpageContents(url)
    id = soup.find_all('input', attrs={'id' : 'hdnCityName'})
    cityName = id[0].get('value')

    if(common.CheckCityValidity(cityName)):
        return cityName
    else:
        return

#---------------------------------------------------------------------------------------------------------------------------
'''
get the 64 digit string unique to each subcategory
<input id="partialPageData" type="hidden" value="eyIkaWQiOiIxIiwiQ2l0eUlkIjoxLCJBcmVhSWQiOjAsIkNhdGVnb3J5SWQiOjMwMywiTmVlZElkIjowLCJOZWVkRmlsdGVyVmFsdWVzIjoiIiwiUm91dGVOYW1lIjoiUmVzdGF1cmFudHMiLCJQYWdlVmlld1R5cGUiOjQsIkhhc0xjZiI6dHJ1ZSwiQnJlYWRDcnVtYlRpdGxlIjoiUmVzdGF1cmFudHMiLCJJc09ubHlQcmltYXJ5VGFnIjpmYWxzZSwiQ2xlYXJDYWNoZSI6ZmFsc2UsIkh1YklkIjoiIiwiQXR0cmlidXRlcyI6IjAiLCJWZXJzaW9uIjoyLCJJc0FkTGlzdGluZ1BhZ2UiOmZhbHNlLCJJc0FkRGV0YWlsUGFnZSI6ZmFsc2UsIlJlZk5lZWRJZCI6MCwiVGVtcGxhdGVOYW1lIjoiIiwiSXNQd2EiOmZhbHNlLCJEaXNhYmxlR29vZ2xlQWRzIjpmYWxzZSwiU3RhdGVDb2RlIjoiVE4iLCJSb3V0ZUlkIjoicmVzdGF1cmFudHMiLCJDaXR5TmFtZSI6IkNoZW5uYWkifQ==">
'''
def GetPartialPageData(url):

	soup = common.GetWebpageContents(url)
	partialPageData = soup.find_all('input', attrs={'id' : 'partialPageData'})
	partialValue = partialPageData[0].get('value')
	if(common.CheckNameValidity(partialValue)):
		return partialValue
	else:
		return

#---------------------------------------------------------------------------------------------------------------------------

def CombineUrl(partialValue,cID,pageNr,subcategoryName,cityName):

    '''
    if SubcategoryName has spaces ex: Real Estate - space should be replaced with '+' symbol instead.
    if SubcategoryName has & ex: Industrial Dry Cleaners & Laundry Services - & should be replaced with %26
    otherwise the url will be invalid
    if no spaces it returns false
    '''
    subcategoryName = urllib.parse.quote_plus(subcategoryName)

    subcategoryBase = '/mvc5/lazy/v1/Listing/get-business-list?PartialPageData='+partialValue
    filters = '&Category='+cID+'&Filter={}&PageNr='+pageNr+'&Sort=&getQuoteVisiblity=&aboutEnabled=&CategoryName='+subcategoryName+'&CityName='+cityName
    remainder = '&IsAboutEnabled=True&fp=0&tp=0&fa=0&ta=0&au=&GroupCityId=0'

    url = subcategoryBase + filters + remainder

    if(common.CheckUrlValidity(url)):
        return common.GetBaseUrl()+url
    else:
        return

#---------------------------------------------------------------------------------------------------------------------------
'''
check if there are more results
if true go to next page and check again
<input type="hidden" id="hdnBizHasMoreResults" value="True">
'''
def GetHasMoreResults(url):

	soup = common.GetWebpageContents(url)
	hasMoreResults = soup.find_all('input', attrs={'id' : 'hdnBizHasMoreResults'})
	result = hasMoreResults[0].get('value')
	if(result == 'True'):
		return True
	else:
		return False

#---------------------------------------------------------------------------------------------------------------------------
'''
count number of pages if hasMoreResults is true
'''
def GetNumberOfPages(partialValue,cID,pageNr,subcategoryName,cityName):

	# checks from 1st page till last page
	while True:
		hasMoreResults = GetHasMoreResults(CombineUrl(partialValue,cID,str(pageNr),subcategoryName,cityName))
		pageNr = int(pageNr) + 1
		if(hasMoreResults is False):
			break

	return pageNr

#---------------------------------------------------------------------------------------------------------------------------
'''
get the contents of the webpage and filter based on this class
<li class="list-item view-r" data-loc="T. Nagar" data-city="Chennai" data-country="IN" data-pincode="600017" data-rating="4.5" data-id="103006" data-type="" data-name="Pizza Hut" data-bvn="+91 9952237947"</li>
'''
def ParseBusinessPage(url):

    soup = common.GetWebpageContents(url)
    businessHeader = soup.find_all('li', attrs={'class' : 'list-item view-r'})
    if(businessHeader is not None):
        return businessHeader

#---------------------------------------------------------------------------------------------------------------------------
'''
<a href="/aqua-green-hotels-resorts-puzhal-chennai-contact-address" class="bizlinkurl GAQ_C_BUSL busi-name" id="3606590" title="Aqua Green Hotels &amp; Resorts in Chennai-600066" tabindex="0"><h3>Aqua Green Hotels &amp; Resorts</h3></a>
'''
def ProcessDataToSave(businessData,subcategoryID):

    for data in businessData:

        businessAnchorTag = data.find_all('a',attrs={'class':'bizlinkurl GAQ_C_BUSL busi-name'}) # anchor tag

        for row in businessAnchorTag:
            businessName = row.find('h3').string
            businessUrl = row['href']
            if(common.CheckNameValidity(businessName) and common.CheckUrlValidity(businessUrl)):
                businessList.append([businessName,businessUrl])
                '''
                Save the SubcategoryID, Business Name and Business Link ( used to get contact info ) into database
                '''
                if(common.CheckNameValidity(businessName) and common.CheckUrlValidity(businessUrl)):
                    SaveBusinessData(subcategoryID,businessName,businessUrl)
            else:
                return
    return businessList

#---------------------------------------------------------------------------------------------------------------------------

def SaveBusinessData(subcategoryID,businessName,businessUrl):

    query = ('insert into BusinessInfo (SubcategoryID,BusinessName,BusinessLink) values (?,?,?)')
    print("Saving ", subcategoryID,businessName,businessUrl)
    result = common.SaveToDatabase(query,( subcategoryID,businessName,businessUrl ))
    return result

#---------------------------------------------------------------------------------------------------------------------------

GetBusinessData()
