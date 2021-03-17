# Parse-XML-to-CSV-upload-to-S3-bucket

**Description**:

**main.py** -> This module conatins the code for downloading the XML files from Web using "requests" library, parsing the same to get useful information using "xml" library and uploading the final results to s3 bucket using "boto3" library.

Logging will be done in "app.log"

The initial XML file will be saved as "Intermediate.XML" which will parsed for the web link of XML containg data. The zip downloaded will be saved as "Data.zip". The final results after parsing of XML data will be stored in "Info.csv". 

**test.py** -> This module uses pytest framework, imports main.py and contains unit tests for different functionalities used in the assignment.

**Note1** - Please add the below information in global variables in main.py, for uploading the result("Info.csv") to s3 bucket otherwise the file will just be saved in local disk.

#####################################

To be Assigned by User

AWS_ACCESS_KEY_ID = ""

AWS_ACCESS_KEY_SECRET = ""

REGION_NAME = ""

BUCKET_NAME = ""

#####################################


**Usage** :

python main.py           ==>      For Executing the whole module  

pytest -v test.py        ==>      For running unit testcases on main.py module

python -m pydoc main     ==>      Documentation for main.py module

python -m pydoc test     ==>      Documentation for test.py module








**Note2** - There are a few points which could have been handled in a better way like handling AWS credentials, arguements validation, etc. But keeping it simple for this assignment. Attached are the few succesful screenshots of the assignment.


![image](https://user-images.githubusercontent.com/17096303/111452725-d0724480-8738-11eb-9268-f410cc9278fb.png)
![result](https://user-images.githubusercontent.com/17096303/111452938-09121e00-8739-11eb-8361-9a0546ef6f6d.PNG)
![s3_result](https://user-images.githubusercontent.com/17096303/111452945-0adbe180-8739-11eb-8273-cf8c6c73491a.PNG)
