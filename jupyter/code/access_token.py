"""
 Copyright (C) 2023 Atlas of Living Australia

 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0. If a copy of the MPL was not distributed with this
 file, You can obtain one at <https://mozilla.org/MPL/2.0/>.
"""
"""
  @author NdR
  @create date 2023-02-28 
  @modify date 2023-02-28 

ALA OIDC access_token generator

See docs.ala.org.au for documentation and tokens.ala.org.au to request new client credentials
Currently, client ID and secret are set via ENV variables or using a `.env` file in the 
repository root directory. 

"""

import http.client
import base64
import sys
import logging
import json
from decouple import config
logging.basicConfig(stream=sys.stdout,
                    format="%(levelname)s %(asctime)s - %(message)s")
logger = logging.getLogger("access_token")
logging.root.setLevel(logging.WARN)


CLIENT_ID = config('CLIENT_ID', default="")
CLIENT_SECRET = config('CLIENT_SECRET', default="")
ALA_AUTH_HOST_NAME = config(
    'ALA_AUTH_HOST_NAME', default="auth.ala.org.au")


def create_token():
    # check to see that client creds are set
    if not CLIENT_ID and not CLIENT_SECRET:
        # TODO: use exception
        msg = "Error: CLIENT_ID and CLIENT_SECRET must be set via ENV var or in `.env` file."
        # print(msg)
        logger.error(msg)
        return None
    scope = "email openid"
    oauth_path = "/cas/oidc/oidcAccessToken"
    conn = http.client.HTTPSConnection(ALA_AUTH_HOST_NAME, 443)
    payload = f"grant_type=client_credentials&scope={scope}"
    headers = {
        'Authorization': 'Basic {}'.format(base64.b64encode(bytes(f"{CLIENT_ID}:{CLIENT_SECRET}", "utf-8")).decode("ascii")),
        'content-type': "application/x-www-form-urlencoded"
    }
    # print("request data:", oauth_path, payload, headers)
    # conn.debuglevel
    conn.request(
        "POST", oauth_path, payload, headers)
    res = conn.getresponse()
    if res.status == 200:
        data = res.read()
        json_data = json.loads(data)
        logger.debug(f"headers: {headers}")
        logger.debug(f"create_token response: {json_data}")
        if json_data["access_token"]:
            access_token = json_data["access_token"]
            return access_token
    else:
        # TODO: throw exception ?
        logger.error(
            f"Error generating access_token: {res.status} -> {res.msg}")


if __name__ == "__main__":
    create_token()
