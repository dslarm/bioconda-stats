#! /usr/bin/env python

from datetime import datetime
from typing import Optional

from ntplib import NTPClient, NTPException

from .common import DATE_FORMAT


def get_ntp_time(client: Optional[NTPClient] = None, date_format: str = DATE_FORMAT) -> str:
    ntp_pool = (
        "0.pool.ntp.org",
        "1.pool.ntp.org",
        "2.pool.ntp.org",
        "3.pool.ntp.org",
    )
    if client is None:
        client = NTPClient()
    errors = []
    for server in ntp_pool:
        try:
            response = client.request(server, version=4, timeout=2)
        except NTPException as e:
            errors.append(e)
            continue
        time = datetime.fromtimestamp(response.tx_time)
        return time.strftime(date_format)
    raise NTPException(f"Could not get timestamp. Errors: {errors}")


if __name__ == "__main__":
    print(get_ntp_time())
