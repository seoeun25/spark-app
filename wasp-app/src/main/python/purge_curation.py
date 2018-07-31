import sys
import requests

# spark-submit wasp-app/src/main/python/purge_curation.py production

def config(env):
    ## 이 파일 하나로 모든 것을 처리 하기 위해 따로 property 파일을 만들지 않는다
    if env == "production":
        prop = {"cms": "https://cms-dot-lezhincomix-api.appspot.com",
                     "cmstoken": "b58b583f-2bac-4035-a8c8-9a8c56e64085"}
    elif env == "mirror":
        prop = {"cms": "https://cms-dot-lezhincomix-mirror.appspot.com",
                "cmstoken": "c63a7469-08dc-41d9-b477-000e195cae87"}
    elif env == "qa":
        prop = {"cms": "https://cms-dot-lezhincomix-qa.appspot.com",
                "cmstoken": "f632a8cc-65f0-48dd-bdae-a63ff2e0470e"}
    else:
        prop = {"cms": "https://localhost", "cmstoken": ""}

    return prop

def send(env):

    print("env=", env)

    prop = config(env)

    print("--- prop", prop)
    host = prop["cms"]
    #host = 'https://cms-dot-lezhincomix-qa.appspot.com'
    token = prop["cmstoken"]

    print(" -- host = ", host)
    print(" -- token = ", token)

    path = '/v2/purge/curation_sets'

    headers = {"Content-Type" : "application/json", "authorization" : "Bearer " + token}

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

    result = send(env)

    print("result = {}".format(result))

    print("Done!")
