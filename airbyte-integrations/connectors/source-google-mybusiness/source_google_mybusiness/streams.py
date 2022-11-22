from abc import ABC
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Union
from urllib.parse import quote_plus, unquote_plus

import pendulum
import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import HttpAuthenticator
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer
from airbyte_cdk.sources.streams.core import Stream

BASE_URL = "https://mybusiness.googleapis.com/"


class GoogleMybusinessStream(HttpStream, ABC):
    url_base = BASE_URL
    primary_key = None
    data_field = ""

    def __init__(
        self,
        authenticator: Union[HttpAuthenticator, requests.auth.AuthBase],
        account_id: str,
        start_date: str,
        end_date: str,
        read_mask: str,
    ):
        super().__init__(authenticator=authenticator)
        self._account_id = account_id
        self._start_date = start_date
        self._end_date = end_date
        self._read_mask = read_mask

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        if not self.data_field:
            yield response.json()

        else:
            records = response.json().get(self.data_field) or []
            for record in records:
                yield record


class Locations(GoogleMybusinessStream):
    data_field = "locations"

    @property
    def http_method(self) -> str:
        return "GET"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        next_page = decoded_response.get("nextPageToken")
        if next_page:
            return {"pageToken": next_page}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        params = {"read_mask": self._read_mask}

        if next_page_token:
            params.update(next_page_token)

        return params

    """
    API docs: https://developers.google.com/my-business/reference/rest/v4/accounts
    """

    def path(self, **kwargs) -> str:
        return f"v1/accounts/{self._account_id}/locations"


class LocationInsights(GoogleMybusinessStream):
    def __init__(self, **kwargs):
        """
        :param parent: should be the instance of HttpStream class
        """
        super().__init__(**kwargs)
        self.location = Locations(**kwargs)
        self.location_list = []
        self.max_number_location = 10

    def request_body_json(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Mapping]:
        return {
            "basicRequest": {"metricRequests": [{"metric": "ALL", "options": ["AGGREGATED_DAILY"]}],
                     "timeRange": {"startTime": self._start_date, "endTime": self._end_date}},
            "locationNames": stream_slice.get("locationNames"),
        }


    @property
    def http_method(self) -> str:
        return "POST"

    def stream_slices(
        self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:

        parent_stream_slices = self.location.stream_slices(
            sync_mode=SyncMode.full_refresh, cursor_field=cursor_field, stream_state=stream_state
        )

        for stream_slice in parent_stream_slices:
            parent_records = self.location.read_records(
                sync_mode=SyncMode.full_refresh, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state
            )

            # iterate over all parent records with current stream_slice
            for record in parent_records:
                self.location_list.append(record["name"])

        # initializing append_str
        append_str = "accounts/{}/".format(self._account_id)

        # Append suffix / prefix to strings in list
        result_location_list = [append_str + sub for sub in self.location_list]

        for i in range(0, len(result_location_list), self.max_number_location):
            yield {"locationNames": [result_location_list[i : i + self.max_number_location]]}

    """
    API docs: https://developers.google.com/my-business/reference/rest/v4/accounts.locations/reportInsights
    """

    def path(self, **kwargs) -> str:
        return f"v4/accounts/{self._account_id}/locations:reportInsights"
