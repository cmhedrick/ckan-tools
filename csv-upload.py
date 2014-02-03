import ckanclient
import ckanclient.datastore
import StringIO
import csv
import sys

ckan_site = 'http://ckan_site_name/api'
api_key = 'your_api_key'
client = ckanclient.CkanClient(ckan_site, api_key)
csvfile = open(sys.argv[1], 'rb')
package_id = 'package_id'
fields = []
data = []

def csv2data(csvfile):
    try:
        col_len = ''
        temp_dic = {}
        reader = csv.reader(csvfile)
        for row in reader:
            if reader.line_num == 1:
                for header in row:
                    fields.append({'id': header})
            else:
                for i in range(len(row)):
                    temp_dic[fields[i]['id']] = row[i]
                data.append(temp_dic)
                temp_dic = {}
        return fields, data

    finally:
        csvfile.close()

pkg = dict(
    name='123',
    title='123',
    resources=[
        dict(
            id=package_id,
            url='made-up-url2',
            name='For Datastore try-out'
            ),
        ]
    )
 
def create_dataset():
    try:
        client.package_register_post(pkg)
    except ckanclient.CkanApiError:
        client.package_entity_put(pkg)
  
def create_datastore():
    client.action('datastore_create',
        resource_id=package_id,
        fields=fields,
        records=data
    )
 
if __name__ == '__main__':
    csv2data(csvfile)
    create_dataset()
    create_datastore()
