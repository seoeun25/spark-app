import itertools
import sys
import time
import requests

# spark-submit wasp-app/src/main/python/purge_curation.py production

def send():

    # qa
    host = 'https://cms-dot-lezhincomix-qa.appspot.com'
    path = '/v2/purge/curation_sets'

    headers = {"Content-Type" : "application/json", "authorization" : "Bearer " + "f632a8cc-65f0-48dd-bdae-a63ff2e0470e"}

    url = "{}{}".format(host, path)
    print("url = ", url)

    r = requests.delete(url, headers=headers)
    return r


if __name__ == "__main__":
    print(sys.argv)
    print(sys.argv.__len__())
    if len(sys.argv) < 1:
        print("Error usage: push_message [env] ")
        print("purge_curation.py qa")
        sys.exit(-1)
    env = sys.argv[1]

    print("env: %s", env)

    result = send()

    print("result = {}".format(result))

    print("Done!")
