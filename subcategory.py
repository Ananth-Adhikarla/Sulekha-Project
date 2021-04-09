import common
import queue
from enum import Enum
import random
import mythreads # custom class
import threading
from signal import signal, SIGINT,SIGTSTP
from sys import exit


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
List to store subcategories in
'''
categoryList = []
cityList = []
canExit = False

def handler(signal_received, frame):
    # Handle any cleanup here

    print('Closing program after thread completes')
    mythreads.StopThreads(canExit)

def CheckSignals():
    signal(SIGINT, handler)
    signal(SIGTSTP, handler)

def GetSubcategoryData():

    global canExit

    ResetThreadStatus()
    canExit = True
    CheckSignals()
    statusCount = mythreads.GetStatusCount(Flags.city)
    while(not statusCount):
        CheckSignals()
        s = mythreads.StartThreads(Flags.city,GetCity,None,cityList)
        statusCount = mythreads.GetStatusCount(Flags.city)
        canExit = s[1]

#    for row in cityList:
#        print(row)

    #city = GetCity() # will be for all cities for now using only 1 - to remove parameters
#    for cityRow in cityList:
#        categoryData = ParseCityPage(common.GetBaseUrl()+cityRow[2])
#        categoryList.append(categoryData)

#    SubcategoryData(cityList,categoryList) # filter the data and check before saving

#---------------------------------------------------------------------------------------------------------------------------
def GetCity(myqueue):

    conn = common.OpenDb()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM City WHERE STATUS=? ORDER BY RANDOM() LIMIT 1",(StatusFlags.free.value,)) # select 1 random city
    cityInfo = cursor.fetchone()
    common.CloseDb(conn)
    data = mythreads.PostThreadStatus(StatusFlags.using.value,cityInfo[0],Flags.city)
    print(data[0],data[1])
    myqueue.put(data)
    mythreads.PostThreadStatus(StatusFlags.finish.value,cityInfo[0],Flags.city)
    return data


#---------------------------------------------------------------------------------------------------------------------------
def ResetThreadStatus(): # only for testing

    conn = common.OpenDb()
    cursor = conn.cursor()
    cursor.execute("Update City Set Status=0 ")
    conn.commit()
    common.CloseDb(conn)

#---------------------------------------------------------------------------------------------------------------------------

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

def SubcategoryData(cityList,categoryList):

    for i in range(len(cityList)):
        for subList in categoryList[i]:

            if(subList.find('h2') is not None): # only get records which is not empty / null
                categoryHeader = subList.find('h2').string
                categoryID = GetCategoryID(categoryHeader)
                subcategoryHeader = subList.find('ul')
                cityID = cityList[i][0]

                for subcategory in subcategoryHeader:
                    subName = subcategory.string
                    subUrl = subcategory.a['href']
                    if(CheckConditions(subName,subUrl)):
                        SaveSubcategory(categoryID,cityID,subName, subUrl)
                    else:
                        print('subcategory already exists')

#---------------------------------------------------------------------------------------------------------------------------

def CheckValidity(subcategoryName,subcategoryUrl):

	isValidSubcategoryName = common.CheckNameValidity(subcategoryName)
	isValidUrl = common.CheckUrlValidity(subcategoryUrl)

	if(isValidSubcategoryName and isValidUrl):
		return True
	else:
		return False

#---------------------------------------------------------------------------------------------------------------------------

def CheckDuplicates(subcategoryName, subcategoryLink):

	query = ("SELECT SubcategoryName,SubcategoryLink FROM Subcategory WHERE SubcategoryName=? AND SubcategoryLink=? ")
	result = common.CheckDuplicates(query,(subcategoryName, subcategoryLink))
	return result

#---------------------------------------------------------------------------------------------------------------------------

def CheckConditions(cityName,cityUrl):

	isValid = CheckValidity(cityName,cityUrl)
	isDuplicate = CheckDuplicates(cityName,cityUrl)

	if(isValid and not isDuplicate):
		return True
	else:
		return False

#---------------------------------------------------------------------------------------------------------------------------

def SaveSubcategory(categoryID, cityID, subcategoryName, subcategoryLink):

	query = ('insert into Subcategory (CategoryID, CityID, SubcategoryName, SubcategoryLink) values (?,?,?,?)')
	result = common.SaveToDatabase(query,( categoryID, cityID, subcategoryName, subcategoryLink ))
	return result

#---------------------------------------------------------------------------------------------------------------------------

GetSubcategoryData()
