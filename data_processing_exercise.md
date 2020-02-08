# Data processing exercise

## Description

Implement data processing task by using streams & join with sliding window technique.

We have 3 type of objects View, Click & Viewable View Event:

View:
* Id
* Logtime 
* Campaign Id

Click:
* Id
* Logtime
* Campaign Id
* Interaction Id (View.Id)

Viewable View Event:
* Id
* Logtime
* Interaction Id (View.Id)

3 distinct files are populated with entities described above:
* Views.csv
* Clicks.csv
* ViewableViewEvents.csv

Your task is to create an application (by using your prefered language: C#, Scala, Java) which will do the following:
1. Accept files as an arguments;
2. Will join View & Click entities & produce new file ViewsWithClicks.csv populated with ViewsWithClicks entity:
	1. Id => View.Id;
	2. Logtime => View.Logtime;
	3. ClickId = Click.Id;
3. Will join View & Viewable View Event & produce new file ViewableViews.csv populated with View entities which were joined;
4. Aggregate statistics from generated files and produce new file statistics.csv in such format:
	1. Campaign Id;
	2. Views (sum);
	3. Clicks (sum); 
	4. Views which were viewable (sum);
	5. Click through rate ((clicks / views) * 100);
  
### Addition requirements
* Data from files should be treated as stream;
* Do not use database type storage in your application (for example: SQLite);
* Your application should be able to handle bigger files ( > 1 GB);
* You should try to use as little memory as possible;

### Details
* Not all views can be joined with clicks - some of them won't have a counterpart;
* Not all views can be joined with viewable view events  - some of them won't have a counterpart;
* You may choose stream join window time that suits your needs the best  