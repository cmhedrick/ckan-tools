import ckanclient
import ckanclient.datastore
import StringIO
import csv
import sys
import os
import subprocess
import re
 
base_location = os.environ['CKAN_INSTANCE'] + '/api'
api_key = os.environ['CKAN_APIKEY']
client = ckanclient.CkanClient(base_location, api_key)
csvfile = open(sys.argv[1], 'rb')

class csv_package(object):

    def __init__(self, csvfile):
        self.dataset_name = ''
        self.dataset_url = ''
        self.datastore_resource_id = ''
        self.fields = []
        self.data = []
	self.create_dataset_info()
        self.csv2data(csvfile)
        self.pkg = {}
	self.pkg_make()
	self.create_resource()

    def create_dataset_info(self):
	while self.dataset_name == '' or re.search(r'\W', self.dataset_name):
            self.dataset_name = raw_input("Name of dataset => ")
        while self.dataset_url == '' or re.search(r'\W', self.dataset_url):
            self.dataset_url = raw_input("Link set to source => ")
        os.system('curl %s/3/action/package_create --data \'{"name":"%s"}\' -H Authorization:%s' % tuple([base_location, self.dataset_name, api_key]))
        self.locate_id()
        return self.dataset_name, self.dataset_url

    def locate_id(self):
        setinfo = os.popen('curl %s/3/action/package_show --data \'{"id":"%s"}\'' % tuple([base_location, self.dataset_name])).read()
        begin_int = setinfo.find('"id"')
        end_int = setinfo[begin_int:].find(',')
        end_id = begin_int + end_int
        self.datastore_resource_id = setinfo[begin_int:end_id].split(': ')[1].strip('"')
        return self.datastore_resource_id

    def create_resource(self):
	os.system('curl %s/3/action/resource_create --data \'{"package_id":"%s", "url":"%s"}\' -H Authorization:%s' % tuple([base_location, self.datastore_resource_id, self.dataset_url, api_key]))

    def csv2data(self, csvfile):
        try:
            col_len = ''
            temp_dic = {}
            reader = csv.reader(csvfile)
            for row in reader:
                if reader.line_num == 1:
                    for header in row:
                        self.fields.append({'id': header})
                else:
                    for i in range(len(row)):
                        temp_dic[self.fields[i]['id']] = row[i]
                    self.data.append(temp_dic)
                    temp_dic = {}
            return self.fields, self.data

        finally:
            csvfile.close()

    def pkg_make(self):
        self.pkg = dict(
            name = self.dataset_name,
            title = self.dataset_name,
            resources = [
                dict(
                    id = self.datastore_resource_id,
                    url = self.dataset_url,
                    name = self.dataset_name
                    ),
                ]
            )
        return self.pkg
 
def create_dataset(package_obj):
    try:
        client.package_register_post(package_obj.pkg)
    except ckanclient.CkanApiError:
        client.package_entity_put(package_obj.pkg)
  
def create_datastore(package_obj):
    client.action('datastore_create',
        resource_id = package_obj.datastore_resource_id,
        fields = package_obj.fields,
        records = package_obj.data
    )
 
if __name__ == '__main__':
    pack = csv_package(csvfile)
    create_dataset(pack)
    create_datastore(pack)
