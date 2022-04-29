######
### THIS SHOULD ONLY BE CALLED ONCE A DAY

import os
import requests
from datetime import date
import requests, zipfile
from io import BytesIO

today = date.today()
d = today.strftime("%Y-%b-%d")

#Defining the zip file URL
url = ' https://sportsdata.io/members/download-file?product=de918b91-6b99-4425-aa3b-de8835d70665'

filename = 'Fantasy.2019-2022.zip'

# Downloading the file by sending the request to the URL
req = requests.get(url)

# extracting the zip file contents
zipfile = zipfile.ZipFile(BytesIO(req.content))
cwd = os.getcwd()
zipfile.extractall(cwd + '/daily-downloads/Fantasy.2019-2022' + d)