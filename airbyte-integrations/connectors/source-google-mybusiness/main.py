#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_google_mybusiness import SourceGoogleMybusiness

if __name__ == "__main__":
    source = SourceGoogleMybusiness()
    launch(source, sys.argv[1:])
