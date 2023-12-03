# Baby-Ni

in order to run this file you should create 6 seprate folders 
ToWatch
ToBeParsed
ToBeLoaded
ParsedData
LoadedData 
Redundant

and replace the directories 
so the project could iterate through them

In the database:
 
 there are 4 extra tables:

**TransactionLogStatus: logs the files pipeline till the aggregation phase 
**AgregationLogStatus: logs data aggrehated to the tables
**ColStatus:to check what column are needed to keep while parsing
**errorLog: to log errors while parsing
