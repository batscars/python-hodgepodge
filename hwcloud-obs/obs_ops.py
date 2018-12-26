import requests
import json
from email.utils import formatdate
from datetime import datetime
from time import mktime
import hmac
import base64
import hashlib


access_key = "TMZSTYUW8HSDUI1NBD7M"
security_key = ""
base_uri = "http://obs.cn-south-1.myhwclouds.com"


def make_digest(message, key):
    key = bytes(key, 'UTF-8')
    message = bytes(message, 'UTF-8')
    digester = hmac.new(key, message, hashlib.sha1)
    signature1 = digester.digest()
    signature2 = base64.urlsafe_b64encode(signature1)    
    return str(signature2, 'UTF-8')


def construct_header(verb="GET", resource="/", content_type=""):
    now = datetime.now()
    stamp = mktime(now.timetuple())
    date = formatdate(timeval=stamp, localtime=False, usegmt=True)
    print(date)
    message = verb + "\n\n" + content_type + "\n" + date + "\n" + resource
    signature = make_digest(message, security_key)
    headers = {
        "Authorization": "OBS " + access_key + ":" + signature,
        "Date": date
    }
    return headers


headers = construct_header()
resp = requests.get(base_uri, headers=headers)
print(resp.text)