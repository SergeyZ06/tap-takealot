"""Stream type classes for tap-takealot."""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.pagination import BasePageNumberPaginator

from tap_takealot.client import TakealotStream

from datetime import datetime


class SalesPaginator(BasePageNumberPaginator):
    def has_more(self, response):
        total = response.json()['page_summary']['total']
        page_size = response.json()['page_summary']['page_size']
        page_number = response.json()['page_summary']['page_number']
        return page_number * page_size < total


class SalesStream(TakealotStream):
    """Define custom stream."""

    name = "sales"
    path = "/v2/sales"
    records_jsonpath = "$.sales[*]"

    primary_keys: t.ClassVar[list[str]] = ["order_id", "order_item_id"]
    replication_key = "order_date"
    
    schema = th.PropertiesList(
        th.Property("order_id", th.IntegerType),
        th.Property("order_item_id", th.IntegerType),
        th.Property("quantity", th.IntegerType),
        th.Property("selling_price", th.NumberType),
        th.Property("order_date", th.DateTimeType),
        th.Property("sku", th.StringType),
        th.Property("product_title", th.StringType),
        th.Property("sale_status", th.StringType)
    ).to_dict()

    def get_new_paginator(self):
        return SalesPaginator(start_value=1)
    
    def get_url_params(self, context, next_page_token):
        params = {}

        if next_page_token:
            params["page_number"] = next_page_token
        else:
            params["page_number"] = 1

        start_date = self.compare_start_date(
            value=self.get_starting_replication_key_value(context=context),
            start_date_value=self.config['start_date']
        ).split()[0]

        end_date = self.config.get("end_date")

        if start_date and end_date:
            params['filters'] = f'start_date:{start_date},end_date:{end_date}'
        elif start_date:
            params['filters'] = f'start_date:{start_date}'
        elif end_date:
            params['filters'] = f'end_date:{end_date}'

        params['page_size'] = self.config.get("page_size")

        return params
    
    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row['order_date'] = datetime.strptime(row['order_date'], '%d %b %Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
        row['sale_status'] = str(row['sale_status'])
        return row
