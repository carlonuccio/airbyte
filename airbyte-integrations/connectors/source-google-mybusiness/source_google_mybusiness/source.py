#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import json
from typing import Any, List, Mapping, Optional, Tuple
from urllib.parse import urlparse

import jsonschema
import pendulum
import requests
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.auth import Oauth2Authenticator
from source_google_mybusiness.service_account_authenticator import ServiceAccountAuthenticator
from source_google_mybusiness.streams import Locations, LocationInsights


# Basic full refresh stream

# Source
class SourceGoogleMybusiness(AbstractSource):
    def _validate_and_transform(self, config: Mapping[str, Any]):
        authorization = config["authorization"]
        if authorization["auth_type"] == "Service":
            try:
                authorization["service_account_info"] = json.loads(authorization["service_account_info"])
            except ValueError:
                raise Exception("authorization.service_account_info is not valid JSON")

        pendulum.parse(config["start_date"])
        end_date = config.get("end_date")
        if end_date:
            pendulum.parse(end_date)
        config["end_date"] = end_date or pendulum.now().to_datetime_string().replace(' ', 'T') +'Z'

        config["account_id"] = config.get("account_id")
        config["read_mask"] = config.get("read_mask")
        return config

    def get_stream_kwargs(self, config: Mapping[str, Any]) -> Mapping[str, Any]:
        return {
            "account_id": config["account_id"],
            "start_date": config["start_date"],
            "end_date": config["end_date"],
            "authenticator": self.get_authenticator(config),
            "read_mask": config["read_mask"],
        }

    def get_authenticator(self, config):
        authorization = config["authorization"]
        auth_type = authorization["auth_type"]

        if auth_type == "Client":
            return Oauth2Authenticator(
                token_refresh_endpoint="https://oauth2.googleapis.com/token",
                client_secret=authorization["client_secret"],
                client_id=authorization["client_id"],
                refresh_token=authorization["refresh_token"],
            )
        elif auth_type == "Service":
            return ServiceAccountAuthenticator(service_account_info=authorization["service_account_info"])

    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        try:
            config = self._validate_and_transform(config)
            stream_kwargs = self.get_stream_kwargs(config)
            locations = Locations(**stream_kwargs)

            next(locations.read_records(sync_mode=SyncMode.full_refresh))
            # next(locations.read_records(sync_mode=SyncMode.full_refresh))
            return True, None
        except Exception as error:
            return (
                False,
                f"Unable to connect to Google Business API with the provided credentials - {repr(error)}",
            )

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        config = self._validate_and_transform(config)
        stream_config = self.get_stream_kwargs(config)

        streams = [Locations(**stream_config), LocationInsights(**stream_config)]

        return streams
