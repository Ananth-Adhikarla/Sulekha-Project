import common

#---------------------------------------------------------------------------------------------------------------------------

def GetCity(cityName):

	conn = common.OpenDb()
	cursor = conn.cursor()
	cursor.execute("SELECT CityLink FROM City WHERE CityName=? ",(cityName,))
	cityLink = cursor.fetchone()
	common.CloseDb(conn)
	return cityLink[0]

#---------------------------------------------------------------------------------------------------------------------------

def GetCategoryData():

	soup = common.GetWebpageContents(common.GetBaseUrl()+GetCity('Chennai'))

	categoryHeader = soup.find_all('div', attrs={'class' : 't-list'})

	for category in categoryHeader:
		#print(category.string, category.a['href'])

		if(category.find('h2') is not None):
			#print(category.find('h2').string)

			strCategoryName = category.find('h2').string

			if(CheckConditions(strCategoryName)):
					SaveCategory(strCategoryName)
			else:
				print('category already exists')

#---------------------------------------------------------------------------------------------------------------------------

def CheckValidity(categoryHeader):

	isValidCategoryName = common.CheckNameValidity(categoryHeader)

	if(isValidCategoryName):
		return True
	else:
		return False


#---------------------------------------------------------------------------------------------------------------------------

def CheckDuplicates(categoryHeader):

	query = ("SELECT CategoryName FROM Category WHERE CategoryName=? ")
	result = common.CheckDuplicates(query,(categoryHeader,))
	return result


#---------------------------------------------------------------------------------------------------------------------------

def CheckConditions(categoryHeader):

	isValid = CheckValidity(categoryHeader)
	isDuplicate = CheckDuplicates(categoryHeader)

	if(isValid and not isDuplicate):
		return True
	else:
		return False

#---------------------------------------------------------------------------------------------------------------------------
def SaveCategory(CategoryName):

	query = ('insert into Category (CategoryName) values (?)')
	result = common.SaveToDatabase(query,( CategoryName, ))
	return result

#---------------------------------------------------------------------------------------------------------------------------

GetCategoryData()
