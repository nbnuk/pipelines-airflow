import sys
from urllib.request import urlopen
import json
import re

print(sys.argv)


def removal_all_except(solr_url, solr_collection_name, to_keep):
    print("Calling " + solr_url)
    print("Going to keep " + to_keep)
    print(solr_url + "/admin/collections?action=LIST&wt=json")

    with urlopen(solr_url + "/admin/collections?action=LIST&wt=json") as url:
        data = json.loads(url.read().decode())
        collections = data["collections"]
        collections.remove(to_keep)
        for collection in collections:
            if bool(re.search(solr_collection_name + '_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{2}', collection)):
                print("Deleting: " + collection)
                with urlopen(solr_url + "/admin/collections?action=DELETE&wt=json&name=" + collection) as deleteUrl:
                    print(json.loads(deleteUrl.read().decode()))
            else:
                print("Skipping: " + collection)
    print("Finished")


removal_all_except(sys.argv[1], sys.argv[2], sys.argv[3])
