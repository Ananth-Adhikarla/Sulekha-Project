import common
import requests
from enum import Enum
from signal import signal, SIGINT,SIGTSTP
import queue
import threading
import sys
import urllib.parse
import time

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
businessList = []
contactList = []

'''
global variable to check if program can exit or not
'''
canExit = False
NUMBER_OF_RECORDS_TO_FETCH = 1

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
    FetchBusinessRecords()

    '''
    UpdateUsingQuery updates the 10 records above to status 1 - being used
    '''
    UpdateUsingQuery()

    '''
    parse the website and fetch the raw data
    '''
    businessData = FetchContactInfo(businessList)

    '''
    UpdateFinishQuery updates the 10 records above to status 2 - finished
    '''
    UpdateFinishQuery()

    '''
    Save the SubcategoryID, Business Name and Business Link ( used to get contact info ) into database
    '''
    SaveBusinessData()

    '''
    Empty the list before going into next loop
    '''
    businessList.clear()
    contactList.clear()


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
def FetchBusinessRecords():

    fetch = [threading.Thread(target=BusinessQuery) for i in range(NUMBER_OF_RECORDS_TO_FETCH)]

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
def BusinessQuery():

    conn = common.OpenDb()
    cursor = conn.cursor()

    cursor.execute("SELECT BusinessId,BusinessLink FROM BusinessInfo Where Status=? ORDER BY RANDOM() LIMIT 1",(StatusFlags.free.value,))

    businessInfo = cursor.fetchone()
    common.CloseDb(conn)

    '''
    put this data into list
    '''
    businessList.append(businessInfo)

    '''
    put this data into usingQueue - to update to status 1
    '''
    usingQueue.put([businessInfo])

    return businessInfo

#---------------------------------------------------------------------------------------------------------------------------
#todo: try without queues and check how it works
def UpdateUsingQuery():

    while not usingQueue.empty():
        info = usingQueue.get()
        conn = common.OpenDb()
        cursor = conn.cursor()
        businessID = info[0]

        cursor.execute("Update BusinessInfo Set Status=? Where BusinessId=?",(str(StatusFlags.using.value),str(businessID[0])))
        conn.commit()

        '''
        put the same record into finishQueue - to update to status 2
        '''
        finishQueue.put([businessID])
        common.CloseDb(conn)

#---------------------------------------------------------------------------------------------------------------------------

def UpdateFinishQuery():

    while not finishQueue.empty():
        info = finishQueue.get()
        conn = common.OpenDb()
        cursor = conn.cursor()
        businessID = info[0]
        cursor.execute("Update BusinessInfo Set Status=? Where BusinessId=?",(str(StatusFlags.finish.value),str(businessID[0])))
        conn.commit()
        common.CloseDb(conn)

#---------------------------------------------------------------------------------------------------------------------------
# only for testing
def ResetThreadStatus():

	conn = common.OpenDb()
	cursor = conn.cursor()
	cursor.execute("Update BusinessInfo Set Status=0 ")
	conn.commit()
	common.CloseDb(conn)

#---------------------------------------------------------------------------------------------------------------------------
'''
Each business listing in general has 3 types of info - Name, Address and Email.
Most listings only provide address and some provide name and very few provide email
'''

def FetchContactInfo(business):

    businessId = business[0][0]
    businessLink = business[0][1]

    pageData = ParseBusinessPage(businessLink)
    print(common.GetBaseUrl()+businessLink)

    for info in pageData:

        personInfo = info.find('div', attrs={'class','person'})
        contactInfo = info.find('div', attrs={'class':'available'})
        emailInfo = info.find('div', attrs={'class':'email'})

        name = GetPersonName(personInfo)
        address = GetAddress(contactInfo)
        email = GetEmail(emailInfo)

        contactList.append([name,address,email,businessId])


    print("\n")

def GetPersonName(personInfo):

    if(personInfo is not None):
        '''
        personInfo.i.extract() will remove the italic tag <div> <i> hello </i> world </div>
        so we only get the div inside it which is <div> world </div>
        '''
        personInfo.i.extract()
        contactName = personInfo.find('span').text
        contactName = RemoveNameSpace(contactName)

        if(contactName is not None):
            if(common.CheckNameValidity(contactName)):
                return contactName
        else:
            return


def GetAddress(contactInfo):

    if(contactInfo is not None):

        address = contactInfo.find('div').text
        if(address is not None):
            if(common.CheckNameValidity(address)):
                return address

        else:
            return

def GetEmail(emailInfo):

    if(emailInfo is not None):

        emailInfo = emailInfo.find_all('span')
        email = emailInfo[1].text
        if(common.CheckEmailValidity(email)):
            return email

'''
when extracting name from website it has spaces in front ex: ("  contactperson")
'''
def RemoveNameSpace(name):

    name = name.lstrip(' ')
    name = name.rstrip(' ')
    return name
#---------------------------------------------------------------------------------------------------------------------------
'''
get the contents of the webpage and filter based on this class
<li class="list-item view-r" data-loc="T. Nagar" data-city="Chennai" data-country="IN" data-pincode="600017" data-rating="4.5" data-id="103006" data-type="" data-name="Pizza Hut" data-bvn="+91 9952237947"</li>
'''
def ParseBusinessPage(url):

    url = common.GetBaseUrl() + url
    soup = common.GetWebpageContents(url)
    contactInfo = soup.find_all('div', attrs={'id' : 'contacts'})
    if(contactInfo is not None):
        return contactInfo

#---------------------------------------------------------------------------------------------------------------------------

def CheckDuplicates(businessName,businessUrl):

    query = ("SELECT BusinessName,BusinessLink FROM BusinessInfo WHERE BusinessName=? AND BusinessLink=? ")
    result = common.CheckDuplicates(query,(businessName,businessUrl))
    return result

def CheckConditions(businessName,businessUrl):

    isNameValid = common.CheckNameValidity(businessName)
    isUrlValid = common.CheckUrlValidity(businessUrl)
    isDuplicate = CheckDuplicates(businessName,businessUrl)

    if(isNameValid and isUrlValid and not isDuplicate):
        return True
    else:
        return False
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
            if(CheckConditions(businessName,businessUrl)):
                businessList.append([subcategoryID,businessName,businessUrl])

            else:
                return
    return businessList

#---------------------------------------------------------------------------------------------------------------------------

def GetTime():
    '''
    time.time will return data in seconds
    1 second = 1000 milliseconds
    '''
    milli_sec = int(round(time.time() * 1000))
    return milli_sec

def SaveBusinessData():

    start = GetTime()

    query = ('UPDATE BusinessInfo Set BusinessContact=?,BusinessAddress=?,BusinessEmail=? WHERE BusinessID=?')
    conn = common.OpenDb()
    cursor = conn.cursor()
    '''
    currently fecthing 1 business record and getting the data and updating the database
    '''
    name = contactList[0][0]
    address = contactList[0][1]
    email = contactList[0][2]
    id = contactList[0][3]
    print("SAVING NAME :",name," ADDRESS : ",address," EMAIL : ",email," BusinessID : ",id)
    cursor.execute(query,(name,address,email,id))
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

    query = ("DELETE FROM BusinessInfo")

    conn = common.OpenDb()
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    common.CloseDb(conn)

GetBusinessData()
#DeleteRows()
