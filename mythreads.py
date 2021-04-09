import common
import threading # for multi-threading
import queue
from enum import Enum
import random
import time
from sys import exit

class Flags(Enum):
    city = 0
    category = 1
    subcategory = 2
    business = 3

class StatusFlags(Enum):
    free = 0
    using = 1
    finish = 2

# Threads Related

def PostThreadStatus(status,id,flags):

    query = PostStatusQuery(flags)
    time.sleep(1)
    postData = common.SaveToDatabase(query,(str(status),str(id)))
PostThreadStatus
    return postData

def GetStatusCount(flags):

    query = GetStatusCountQuery(flags)
    statusCount = common.GetRowCount(query)
    if(statusCount == 0):
        return True
    else:
        return False

def FetchQuery(flags):

    if(flags.value == Flags.city.value):
        query = ("Select * FROM City Where CityID=?")
        return query
    elif(flags.value == Flags.subcategory.value):
        query = ("Select * FROM Subcategory Where SubcategoryID=?")
        return query
    else:
        return

#---------------------------------------------------------------------------------------------------------------------------
def PostStatusQuery(flags):

    if(flags.value == Flags.city.value):
        query = ("Update City Set Status=? Where CityID=?")
        return query

    elif(flags.value == Flags.category.value):
        query = ("Update Category Set Status=? Where CategoryID=?")
        return query

    elif(flags.value == Flags.subcategory.value):
        query = ("Update Subcategory Set Status=? Where SubcategoryID=?")
        return query

    elif(flags.value == Flags.business.value):
        query = ("Update BusinessInfo Set Status=? Where BusinessID=?")
        return query

    else:
        return

#---------------------------------------------------------------------------------------------------------------------------
def GetStatusCountQuery(flags):

    if(flags.value == Flags.city.value):
        query = ("select count(Status) from City Where Status=0")
        return query

    elif(flags.value == Flags.category.value):
        query = ("select count(Status) from Category Where Status=0")
        return query

    elif(flags.value == Flags.subcategory.value):
        query = ("select count(Status) from Subcategory Where Status=0")
        return query

    elif(flags.value == Flags.business.value):
        query = ("select count(Status) from BusinessInfo Where Status=0")
        return query

    else:
        return

#---------------------------------------------------------------------------------------------------------------------------
def StartThreads(flags,function,args,myList):

    canExit = False
    myqueue = queue.Queue()

    t1 = threading.Thread(target=function, args=(myqueue,) )

    t2 = threading.Thread(target=function, args=(myqueue,) )


    t1.start() # the start() method starts a thread by calling the run method.

    time.sleep(1) # to give 1 second time before 2nd thread starts

    t2.start()

    t1.join() # The join() waits for threads to terminate.
    t2.join()

    while not myqueue.empty(): # The empty( ) returns True if queue is empty; otherwise, False.
        result = myqueue.get() # The get() removes and returns an item from the queue.
        myList.append(result)


    myList.sort()
    canExit = True
    return myList,canExit

def StopThreads(flag):

    if(flag is True):
        exit(0)
