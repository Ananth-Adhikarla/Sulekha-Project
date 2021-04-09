import common

city_names = ['Albany','Albuquerque','Alexandria','Ann Arbor','Antioch','Apple Valley','Athens','Atlanta','Atlantic City','Augusta','Aurora',
              'Baton Rouge','Beaumont','Fort Collins','Fort Lauderdale','Fort Smith','Bel Air','Bellevue']

random_names = ['a10','Chennai_A','Test@123','    ', '@!#$#@!]']

number_test = ['1234','ab12','  123','@1434','!@$%^','    ','3.4125']

url = '/mvc5/lazy/v1/Listing/get-business-list?PartialPageData=eyIkaWQiOiIxIiwiQ2l0eUlkIjo0LCJBcmVhSWQiOjAsIkNhdGVnb3J5SWQiOjMsIk5lZWRJZCI6MCwiTmVlZEZpbHRlclZhbHVlcyI6IiIsIlJvdXRlTmFtZSI6IkFUTXMiLCJQYWdlVmlld1R5cGUiOjQsIkhhc0xjZiI6ZmFsc2UsIkJyZWFkQ3J1bWJUaXRsZSI6IkFUTXMiLCJJc09ubHlQcmltYXJ5VGFnIjpmYWxzZSwiQ2xlYXJDYWNoZSI6ZmFsc2UsIkh1YklkIjoiIiwiQXR0cmlidXRlcyI6IjAiLCJWZXJzaW9uIjoyLCJJc0FkTGlzdGluZ1BhZ2UiOmZhbHNlLCJJc0FkRGV0YWlsUGFnZSI6ZmFsc2UsIlJlZk5lZWRJZCI6MCwiVGVtcGxhdGVOYW1lIjoiIiwiSXNQd2EiOmZhbHNlLCJEaXNhYmxlR29vZ2xlQWRzIjpmYWxzZSwiU3RhdGVDb2RlIjoiS0EiLCJSb3V0ZUlkIjoiYXRtcyIsIkNpdHlOYW1lIjoiQmFuZ2Fsb3JlIn0=&Category=3&Filter={}&PageNr=1&Sort=&getQuoteVisiblity=&aboutEnabled=&CategoryName=ATMs&CityName=Bangalore&IsAboutEnabled=True&fp=0&tp=0&fa=0&ta=0&au=&GroupCityId=0'

url2 = '/mvc5/lazy/v1/Listing/get-business-list?PartialPageData=eyIkaWQiOiIxIiwiQ2l0eUlkIjo0LCJBcmVhSWQiOjAsIkNhdGVnb3J5SWQiOjEzLCJOZWVkSWQiOjAsIk5lZWRGaWx0ZXJWYWx1ZXMiOiIiLCJSb3V0ZU5hbWUiOiJSZWFsIEVzdGF0ZSBBcHByYWlzYWwiLCJQYWdlVmlld1R5cGUiOjQsIkhhc0xjZiI6dHJ1ZSwiQnJlYWRDcnVtYlRpdGxlIjoiUmVhbCBFc3RhdGUgQXBwcmFpc2FsIiwiSXNPbmx5UHJpbWFyeVRhZyI6ZmFsc2UsIkNsZWFyQ2FjaGUiOmZhbHNlLCJIdWJJZCI6IiIsIkF0dHJpYnV0ZXMiOiIwIiwiVmVyc2lvbiI6MiwiSXNBZExpc3RpbmdQYWdlIjpmYWxzZSwiSXNBZERldGFpbFBhZ2UiOmZhbHNlLCJSZWZOZWVkSWQiOjAsIlRlbXBsYXRlTmFtZSI6IiIsIklzUHdhIjpmYWxzZSwiRGlzYWJsZUdvb2dsZUFkcyI6ZmFsc2UsIlN0YXRlQ29kZSI6IktBIiwiUm91dGVJZCI6InJlYWwtZXN0YXRlLWFwcHJhaXNhbC1zZXJ2aWNlcyIsIkNpdHlOYW1lIjoiQmFuZ2Fsb3JlIn0=&Category=13&Filter={}&PageNr=1&Sort=&getQuoteVisiblity=&aboutEnabled=&CategoryName=Real Estate Appraisal&CityName=Bangalore&IsAboutEnabled=True&fp=0&tp=0&fa=0&ta=0&au=&GroupCityId=0'

'''
print("City Validity Test ")
# City Validity Test

for i in range(len(city_names)): # prints true for city
    print(city_names[i]," ",common.CheckCityValidity(city_names[i]))

for i in range(len(random_names)): # prints false for random
    print(random_names[i]," ",common.CheckCityValidity(random_names[i]))

print("----------------------------------------------------------------------------------------------------\n\n")

# Name validity Test

print("Name Validity Test ")
for i in range(len(city_names)): # prints true for city
    print(city_names[i]," ",common.CheckNameValidity(city_names[i]))

for i in range(len(random_names)): # prints true for random and false for ['    ', '@!#$#@!]' - blank space and special chars
    print(random_names[i]," ",common.CheckNameValidity(random_names[i]))

print("----------------------------------------------------------------------------------------------------\n\n")


print("Number Validity Test ")
# Number Validity Test

for i in range(len(number_test)): # prints true only for '1234' others false
    print(number_test[i]," ",common.CheckNumberValidity(number_test[i]))

print("----------------------------------------------------------------------------------------------------\n\n")

print("Url Validity Test ")
print(common.CheckUrlValidity(url2))
'''
