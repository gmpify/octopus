import requests
import re

from pymongo import MongoClient

CONTENT_URL = 'https://store.playstation.com/valkyrie-api/pt/br/19/resolve/UP9000-CUSA00552_00-THELASTOFUS00000'

def extract_region(url):
	region_regex = re.compile('valkyrie-api\/(\w*)\/(\w*)\/')
	region_tuple = region_regex.findall(url)
	if len(region_tuple) != 1:
		print("ERROR C002 Could not parse region from URL")
		raise
	language = region_tuple[0][0].lower()
	country = region_tuple[0][1].lower()
	return language, country

def extract_title_id(content_id):
	title_id_regex = re.compile('-([^_]*)_')
	title_id = title_id_regex.findall(content_id)
	if len(title_id) != 1:
		print("ERROR C003 Could not parse Title ID from Content ID")
		raise
	return title_id[0]

def retrieve_content(url):
	response = requests.get(url)

	response_json = response.json()

	if len(response_json['data']['relationships']['children']['data']) != 1:
		print("ERROR C001 Content does not have one children")
		raise

	content = {}
	content['content_id'] = response_json['data']['relationships']['children']['data'][0]['id']

	for included in response_json['included']:
		if included['id'] == content['content_id']:
			content['name'] = included['attributes']['name']
			content['thumbnail'] = included['attributes']['thumbnail-url-base']

			content['title_id'] = extract_title_id(content['content_id'])
			content['language'], content['country'] = extract_region(CONTENT_URL)
			content['url'] = 'https://store.playstation.com/%s-%s/product/%s' % (content["language"], content["country"], content["content_id"])
			
			break

	return content

def save_content(content):
	mongo_client = MongoClient('mongo', 27017)
	octopus_db = mongo_client.octopus
	content_collection = octopus_db.content

	content_id = content_collection.insert_one(content).inserted_id

	return content_id

content = retrieve_content(CONTENT_URL)
content_id = save_content(content)
print(content_id)
