from pymongo import MongoClient
import urllib.request
import re
import os
import json
import pprint
import codecs
import sys

db_location = 'localhost:27017'
client = MongoClient(db_location)
db = client.hw2
error_array = []

def open_url_split_response(url):
    with urllib.request.urlopen(url) as response:
        html_lines = str(response.read())[1:-1]
        html_lines = html_lines.split("\\n")
        return html_lines

github_url = "https://github.com/opendatajson/factbook.json"
html_lines = open_url_split_response(github_url)
json_region_dirs = filter(lambda x: x.find("href")>=0 and x.find("tree/master/")>=0, html_lines)  # if href string is found
json_region_dirs = list(json_region_dirs)
# GET LIST OF REGIONS' DIRECTORY
for i in range(len(json_region_dirs)):
    html_tag_contents = re.split('< | > | ',json_region_dirs[i].strip())
    for tag_content in html_tag_contents:
        if(tag_content.find("href")>=0):
            json_region_dirs[i] = tag_content

# DOWNLOAD ALL LINKS FOR EVERY REGIONAL DIRECTORY
for json_path in json_region_dirs:
    github_prefix = "https://github.com"
    suffix = json_path.lstrip("href=")[1:-1]
    json_url = github_prefix + suffix
    json_name = suffix.split("/")[-1]
    print("__________________________________________________________________________________")
    print("GET://   json_url : " + json_url + " json_name: " + json_name)
    print("__________________________________________________________________________________")
    html_lines = open_url_split_response(json_url)
    # build regex, inserting the parent continent name var into regex path
    regex = r"[/]master[/]" + re.escape(json_name) + r"[/]\w+[.]json"
    regex = re.compile(regex)
    json_terminal_dirs = []
    for line in html_lines:
        matching_str_list = re.findall(regex,line)
        if(len(matching_str_list) > 0):
            for match in matching_str_list:
                json_terminal_dirs.append(match)
    # ALTERNATE FILTER: NOT AS EFFECTIVE
    #json_terminal_dirs = filter(lambda x: x.find("/master/" + json_name + "/")>=0 and x.find(".json")>=0, html_lines)  # if href string is found
    print("REGEX FOUND A JSON HREF FILE PATH:")
    print("--------------------------------------------------------------------------------------------")
    for i in json_terminal_dirs:
        print(i)
    print("---------------------------------------------------------------------------------------------")

    ### ABANDONED CODE SNIPPET
    '''
    ## GET LINKS TO THE JSON FILES FOR CURRENT REGIONAL DIRECTORY
    for i in range(len(json_terminal_dirs)):
        html_tag_contents = re.split('< | > | ',json_terminal_dirs[i].strip())
        for tag_content in html_tag_contents:
            if(tag_content.find("href")>=0):
                json_terminal_dirs[i] = tag_content
    '''

    ## FOR EACH JSON FILE LINK, CONVER TO RAW GITHUB URL AND DOWNLOAD
    for json_path in json_terminal_dirs:
        file_name = json_path.split("/")[-1]
        github_raw_base = "https://raw.githubusercontent.com"
        repo_name = "/opendatajson/factbook.json"
        raw_github_url = github_raw_base + repo_name +  json_path
        print("GETTING URL: " + raw_github_url)
        data = urllib.request.urlopen(raw_github_url).read().decode()
        json_data = json.loads(data)
        # PERFORM INSERT OPERATION TO THE DATABASE
        try:
            id_str = db.factbook.insert_one(json_data).inserted_id
            print(id_str)
        except:
            err_msg = "ERROR: Could not insert object for json from " + raw_github_url +"\n" + str(sys.exc_info()[0])
            print(err_msg)
            error_array.append(err_msg)

print("FINISHED INSERTING TO MONGODB")
print("No. Failed inserts:" + str(len(error_array)))
print("Displaying failed inserts...\n")
for err in error_array:
    print(err)
