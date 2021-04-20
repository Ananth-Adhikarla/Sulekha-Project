from bs4 import BeautifulSoup
import requests
import sqlite3
import re
import validators
from enum import Enum

class Flags(Enum):
	fetchOne = 1
	fetchAll = 2
	commit = 3

#-----------------------------------------------------------------------------
# DB Related

def GetDatabaseFilePath():
	filepath = '/home/ananth/Desktop/Sulekha/Database/sulekha.sqlite3'
	return filepath

'''
The default value for the timeout parameter is 5.0 (five seconds).
Whenever you connect to SQLite from Python, and you didn’t get a response within 5 seconds your program will raise an exception
sqlite3 supports three locking modes, called isolation levels, that control the locks used to prevent incompatible changes between connections.

By default, check_same_thread is True and only the creating thread may use the connection.
If set False, the returned connection may be shared across multiple threads.
When using multiple threads with the same connection writing operations should be serialized by the user to avoid data corruption.
'''
def OpenDb():
	conn = sqlite3.connect(GetDatabaseFilePath(),timeout=10.0,check_same_thread=False)
	return conn

def CloseDb(conn):
	conn.close()

#-----------------------------------------------------------------------------
# URL + Webpage related
def GetBaseUrl():
	baseUrl = "https://www.sulekha.com"
	return baseUrl

def GetUrl():
	url = "https://www.sulekha.com/local-services/"
	return url

def GetWebpageContents(url):
	html_content = requests.get(url).text
	soup = BeautifulSoup(html_content, "lxml")
	return soup

#-----------------------------------------------------------------------------
#Checking Validity

'''
	checks for city using regular expression.
	re.search = The search() function searches the string for a match, and returns a Match object if there is a match.
				If there is more than one match, only the first occurrence of the match will be returned
	Expression: ^	Starts with
				[a-zA-Z]	Returns a match for any character alphabetically between a and z, lower case OR upper case
				\u0080-\u0024F checks for latin characters
				(Occurrence Indicators)?: zero or one (optional), e.g., [+-]? matches an optional "+", "-", or an empty string.
				(OR Operator) | : E.g., the regex four|4 accepts strings "four" or "4".
				Tested in Test File works fine
'''
def CheckCityValidity(cityName):

	if(len(cityName) > 0): # check if length of string > 0

		regex = re.search("^([a-zA-Z\u0080-\u024F]+(?:. |-| |'))*[a-zA-Z\u0080-\u024F]*$",cityName)
		if(regex):
			 return True
		else:
			return False

	return False

'''
https://validators.readthedocs.io/en/latest/
import validators package
'''
def CheckUrlValidity(UrlName):

	if(len(UrlName) > 0): # check if length of string > 0

		isValid = validators.url(GetBaseUrl()+UrlName,public=True)
		if(isValid):
			return True
		else:
			print(isValid)
			return False

	return False

def CheckEmailValidity(emailID):

	if(len(emailID) > 0):
		isValid = validators.email(emailID)
		if(isValid):
			return True
		else:
			return False
	return False

'''
check for category and subcategory names - tested in Test file works fine
'''
def CheckNameValidity(headerName):

	if(len(headerName) > 0): # check if length of string > 0
		#If you only want to accept alphanumerical characters
		removeSpecialCharacters = re.sub("[^A-Za-z0-9]","",headerName)
		isValid = re.match("^[a-zA-Z0-9]+$",removeSpecialCharacters)
		if(isValid):
			return True
		else:
			return False

	return False

#check for numbers
def CheckNumberValidity(number):

	#isdigit method returns True if all the characters are digits, otherwise False.

	if(number.isdigit()):
		return True
	else:
		return False

#---------------------------------------------------------------------------------------------------------------------------
def CheckDuplicates(query,params):

			# query city table and fetch record if its empty then save data without checking for duplicates
			if(ExecuteDatabaseQuery(query,params,Flags.fetchOne) is None):

				return False # empty
			else:

				return True # not empty

#---------------------------------------------------------------------------------------------------------------------------
'''
ExecuteDatavaseQuery - takes a SQL Query passed from other scripts , parameters and flags
if flag value is 1 then fetch one result
if flag value is 2 then fetch all result
if flag value is 3 then save to database
'''
def ExecuteDatabaseQuery(query,params,flag):

	try:
		conn = OpenDb()
		cursor = conn.cursor()
		cursor.execute(query,params)

		if(flag.value == 1):
			result = cursor.fetchone()
			CloseDb(conn)
			return result
		elif(flag.value == 2):
			result = cursor.fetchall()
			CloseDb(conn)
			return result
		elif(flag.value == 3):
			conn.commit()
			CloseDb(conn)
			return cursor
		else:
			return

	except sqlite3.Error as msg:
		#print(msg)
		return msg


def FetchOneDataAll(query,params):
	data = ExecuteDatabaseQuery(query,params,Flags.fetchOne)
	return data

def FetchOneData(query,params):

	data = ExecuteDatabaseQuery(query,params,Flags.fetchOne)
	return data[0]

def GetRowCount(query):

	try:
		conn = OpenDb()
		cursor = conn.cursor()
		cursor.execute(query)
		count = cursor.fetchone()
		CloseDb(conn)
		return count[0]

	except sqlite3.Error as msg:
		return msg

def SaveToDatabase(query,params):

	cursor = ExecuteDatabaseQuery(query,params,Flags.commit)
	if(cursor.rowcount == 1):
		return True
	else:
		return False




#---------------------------------------------------------------------------------------------------------------------------
