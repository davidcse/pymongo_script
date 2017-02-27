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
db.factbook.insert()

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
#    print("GET://   json_url : " + json_url + " \tjson_name: " + json_name)
    html_lines = open_url_split_response(json_url)
    json_terminal_dirs = filter(lambda x: x.find("/master/" + json_name + "/")>=0 and x.find(".json")>=0, html_lines)  # if href string is found
    json_terminal_dirs = list(json_terminal_dirs)
    ## GET LINKS TO THE JSON FILES FOR CURRENT REGIONAL DIRECTORY
    for i in range(len(json_terminal_dirs)):
        html_tag_contents = re.split('< | > | ',json_terminal_dirs[i].strip())
        for tag_content in html_tag_contents:
            if(tag_content.find("href")>=0):
                json_terminal_dirs[i] = tag_content
    ## FOR EACH JSON FILE LINK, CONVER TO RAW GITHUB URL AND DOWNLOAD
    for json_file in json_terminal_dirs:
        json_path = json_file.strip("href=")[1:-1].split("/")
        file_name = json_path[-1]
        # remove extra tokens that are not relevant
        json_path.remove("blob")
        json_path.remove("")
        # rebuild the path, for raw github download
        json_path = "/".join(json_path)
        raw_github_url = "https://raw.githubusercontent.com/" + json_path
        # download the file and save it to directory at file_name
        urllib.request.urlretrieve(raw_github_url, file_name)
        # open file, read its data, then remove the file.
        jso_file_pointer = open(file_name,'rb')
        json_data = jso_file_pointer.read()
        jso_file_pointer.close()
        os.remove(file_name)
        # print to see what we got
        sys.stdout.buffer.write(json_data)
