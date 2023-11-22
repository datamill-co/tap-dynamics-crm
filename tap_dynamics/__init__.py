#!/usr/bin/env python3

import sys
import json
from datetime import datetime, timedelta

import requests
import singer
from odata import ODataService

from tap_dynamics.discover import discover
from tap_dynamics.sync import sync, sync_pick_lists

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "domain",
    "client_id",
    "client_secret",
    "redirect_uri",
    "refresh_token",
]


class InvalidCredentials(Exception):
    pass


class DynamicsAuth(requests.auth.AuthBase):
    def __init__(self, config):
        self.__resource = "https://{}.dynamics.com".format(config["domain"])
        self.__client_id = config["client_id"]
        self.__client_secret = config["client_secret"]
        self.__redirect_uri = config["redirect_uri"]
        self.__refresh_token = config["refresh_token"]

        self.__session = requests.Session()
        self.__access_token = None
        self.__expires_at = None

    def ensure_access_token(self):
        if self.__access_token is None or self.__expires_at <= datetime.utcnow():
            response = self.__session.post(
                "https://login.microsoftonline.com/common/oauth2/token",
                data={
                    "client_id": self.__client_id,
                    "client_secret": self.__client_secret,
                    "redirect_uri": self.__redirect_uri,
                    "refresh_token": self.__refresh_token,
                    "grant_type": "refresh_token",
                    "resource": self.__resource,
                },
            )
            if response.status_code == 400:
                error = response.json()
                if error.get("error") == "invalid_grant":
                    raise InvalidCredentials(error)

            if response.status_code != 200:
                raise Exception(response.text)

            data = response.json()

            self.__access_token = data["access_token"]

            self.__expires_at = datetime.utcnow() + timedelta(
                seconds=int(data["expires_in"]) - 10
            )  # pad by 10 seconds for clock drift

    def __call__(self, r):
        self.ensure_access_token()
        r.headers["Authorization"] = "Bearer {}".format(self.__access_token)
        return r


@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    url = "https://{}.dynamics.com/api/data/v9.2/".format(parsed_args.config["domain"])
    auth = DynamicsAuth(parsed_args.config)
    try:
        service = ODataService(url, reflect_entities=True, auth=auth)
    except InvalidCredentials as e:
        LOGGER.error(e)
        sys.exit(5)

    catalog = discover(
        service, parsed_args.config.get("advanced_features_enabled", False)
    )
    sync(service, catalog.streams, parsed_args.state, parsed_args.config["start_date"])

    # pick lists items can not be queried via the OData service, so we need to sync them separately
    # we always want to sync this data for all customers
    sync_pick_lists(auth)


if __name__ == "__main__":
    main()
