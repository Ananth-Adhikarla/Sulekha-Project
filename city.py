import common

#---------------------------------------------------------------------------------------------------------------------------
cityList = [] # empty city list to store data not added to database due to error
def GetCityData():

	#This will get the web page contents from url = "https://www.sulekha.com/local-services/"

	soup = common.GetWebpageContents(common.GetUrl())

	'''
	This will find all divs with class name: sd-menu right taken from the code above and find <ul> tags and all <li> tags inside
	'''
	CityHeaders = soup.find('div', attrs={'class':'sd-menu right'}).find('ul').find_all('li')

	'''
	for each row in CityHeaders get the city name and city url
	check conditions for duplicates and validity if good then save data to database otherwise store in empty list
	'''
	for row in CityHeaders:
		strCityName = row.string
		strCityURL = row.a['href']

		if(CheckConditions(strCityName, strCityURL)):
				SaveCity(strCityName, strCityURL)
		else:
			cityList.append([strCityName,strCityURL])

	if(len(cityList) == 0):
		return
	else:
		print(cityList)
#---------------------------------------------------------------------------------------------------------------------------
# Validity checked in common file

def CheckCityValidity(cityName,cityUrl):

	isValidCity = common.CheckCityValidity(cityName)
	isValidUrl = common.CheckUrlValidity(cityUrl)

	if(isValidCity and isValidUrl):
		return True
	else:
		return False

#---------------------------------------------------------------------------------------------------------------------------
# duplicate check in common file - takes 2 parameters = sql query and parameters as tuple

def CheckCityDuplicates(cityName,cityUrl):

	query = ("SELECT CityName,CityLink FROM City WHERE CityName=? AND CityLink=? ")
	result = common.CheckDuplicates(query,(cityName, cityUrl))
	return result

#---------------------------------------------------------------------------------------------------------------------------

def CheckConditions(cityName,cityUrl):

	isValid = CheckCityValidity(cityName,cityUrl)
	isDuplicate = CheckCityDuplicates(cityName,cityUrl)

	if(isValid and not isDuplicate):
		return True
	else:
		return False

#---------------------------------------------------------------------------------------------------------------------------

def SaveCity(strCity,strCityURL):

	query = ('insert into City (CityName, CityLink) values (?,?)')
	result = common.SaveToDatabase(query,(strCity, strCityURL))

	return result

#---------------------------------------------------------------------------------------------------------------------------

GetCityData()
