from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.exceptions import NotFoundError
import ssl
from elasticsearch.connection import create_ssl_context
import os
import numpy as np

import logging
logger = logging.getLogger('alhena_loading')


DEFAULT_MAPPING = {
    "settings": {
        "index": {
            "max_result_window": 100000
        }
    },
    'mappings': {
        "dynamic_templates": [
            {
                "string_values": {
                    "match": "*",
                    "match_mapping_type": "string",
                    "mapping": {
                        "type": "keyword"
                    }
                }
            }
        ]
    }
}


class ES(object):
    """Alhena Elasticsearch connection"""

    DASHBOARD_ENTRY_INDEX = "analyses"

    def __init__(self, host, port):
        """Create a new instance."""
        assert os.environ['ALHENA_ES_USER'] is not None and os.environ[
            'ALHENA_ES_PASSWORD'] is not None, 'Elasticsearch credentials missing'

        ssl_context = create_ssl_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        es = Elasticsearch(hosts=[{'host': host, 'port': port}],
                           http_auth=(os.environ['ALHENA_ES_USER'],
                                      os.environ['ALHENA_ES_PASSWORD']),
                           scheme='https',
                           timeout=300,
                           ssl_context=ssl_context)

        self.es = es

        # Generic load/delete

        def load_record(self, record, record_id, index, mapping=None):
            """Load individual record"""
            if not self.es.indices.exists(index):
                self.create_index(index, mapping=mapping)

            logger.info('Loading record')
            self.es.index(index=index, id=record_id, body=record)

        def load_df(self, df, index_name, batch_size=1e5):
            """Batch load dataframe"""
            total_records = df.shape[0]
            num_records = 0

            for batch_start_idx in range(0, df.shape[0], batch_size):
                batch_end_idx = min(batch_start_idx + batch_size, df.shape[0])
                batch_data = df.loc[df.index[batch_start_idx:batch_end_idx]]

                clean_fields(batch_data)

                records = []
                for record in batch_data.to_dict(orient='records'):
                    clean_nans(record)
                    records.append(record)

                self.load_records(records, index_name)
                num_records += batch_data.shape[0]
                logger.info("Loading %d records. Total: %d / %d (%f%%)", len(
                    records), num_records, total_records, round(num_records * 100 / total_records, 2))
            if total_records != num_records:
                raise ValueError(
                    'mismatch in {num_records} records loaded to {total_records} total records')

        def load_records(self, records, index, mapping=None):
            """Load batch of records"""
            if not self.es.indices.exists(index):
                self.create_index(index, mapping=mapping)

            for success, info in helpers.parallel_bulk(self.es, records, index=index):
                if not success:
                    #   logging.error(info)
                    logger.info(info)
                    logger.info('Doc failed in parallel loading')

        def create_index(self, index_name, mapping=None):
            logger.info('Creating index with name %s', index_name)

            if mapping is None:
                mapping = DEFAULT_MAPPING

            self.es.indices.create(index=index_name,
                                   body=mapping)

        def delete_index(self, index):
            if self.es.indices.exists(index):
                self.es.indices.delete(index=index, ignore=[400, 404])

        def delete_records_by_dashboard_id(self, index, dashboard_id):
            if self.es.indices.exists(index):
                query = get_query_by_dashboard_id(dashboard_id)
                self.es.delete_by_query(index=index, body=query, refresh=True)

        def delete_dashboard_record(self, dashboard_id):
            """Delete individual dashboard record"""
            logger.info("Deleting analysis %s", dashboard_id)
            self.delete_records_by_dashboard_id(
                self.DASHBOARD_ENTRY_INDEX, dashboard_id)

        def is_loaded(self, dashboard_id):
            """Return true if analysis with dashboard_id exists"""

            query = get_query_by_dashboard_id(dashboard_id)
            count = self.es.count(
                body=query, index=self.DASHBOARD_ENTRY_INDEX)

            return count["count"] == 1

        # Views

        def get_views(self):
            """Returns list of all view names"""
            response = self.security.get_role()
            views = [response_key[:-len("_dashboardReader")] for response_key in response.keys(
            ) if response_key.endswith("_dashboardReader")]

            return views

        def is_view_exist(self, view):
            """Returns true if view name exists"""
            view_name = f'{view}_dashboardReader'

            try:
                result = self.es.security.get_role(name=view_name)
                return view_name in result

            except NotFoundError:
                return False

        def verify_dashboards_loaded(self, dashboards):
            """Checks that all dashboards are loaded"""
            unloaded_dashboards = [
                dashboard for dashboard in dashboards if not self.is_loaded(dashboard)]

            assert len(
                unloaded_dashboards) == 0, f"Dashboards are not loaded: {unloaded_dashboards}"

        def add_view(self, view, dashboards=None):
            """Adds a new view"""
            view_name = f'{view}_dashboardReader'

            assert not self.is_view_exist(
                view_name), f'View with name {view} already exists'

            if dashboards is None:
                dashboards = []

            self.verify_dashboards_loaded(dashboards)

            self.es.security.put_role(name=view_name, body={'indices': [{
                'names': [self.DASHBOARD_ENTRY_INDEX] + dashboards,
                'privileges': ["read"]
            }]})

            logger.info('Added new view: %s', view)

        def add_dashboards_to_view(self, view, dashboards):
            """Add aa list of dashboard IDs to a particular view"""
            assert self.is_view_exist(
                view), f'View with name {view} does not exist'

            self.verify_dashboards_loaded(dashboards)

            view_name = f'{view}_dashboardReader'
            view_data = self.es.security.get_role(name=view_name)

            view_indices = list(
                view_data[view_name]["indices"][0]["names"]) + dashboards

            self.es.security.put_role(name=view_name, body={
                'indices': [{
                    'names': list(set(view_indices)),
                    'privileges': ["read"]
                }]
            })

            logger.info('Added %d dashboards to view %s',
                        len(dashboards), view)

        def add_dashboard_to_views(self, dashboard, views):
            """Add a dashboard to all given views"""
            assert self.is_loaded(
                dashboard), f"Dashboard with id {dashboard} is not loaded, please load first"
            nonexistant_views = [
                view for view in views if not self.is_view_exist(view)]

            assert len(
                nonexistant_views) == 0, f"Views do not exist: {nonexistant_views}"

            for view in views:
                self.add_dashboards_to_views(view, [dashboard])

        def remove_view(self, view):
            """Removes view"""
            view_name = f'{view}_dashboardReader'

            self.es.security.delete_role(view_name)
            logger.info('Removed view %s', view)

        def remove_dashboard_from_views(dashboard_id, views=None):
            """Remove dashboard_id from views if specified, all views if not"""
            if views is None:
                logger.info("Fetching all views")
                response = self.es.security.get_role()
                views = [response_key for response_key in response.keys(
                ) if response_key.endswith("_dashboardReader")]

            else:
                views = [f"{view}_dashboardReader" for view in views]

            logger.info("Checking removal of %s from %d views",
                        dashboard_id, len(views))

            for view in views:
                response = self.es.security.get_role(name=view)
                view_data = response[view]

                view_indices = list(view_data["indices"][0]["names"])

                if dashboard_id in view_indices:
                    logger.info("Removing from %s", view)

                    view_indices.remove(dashboard_id)

                    self.es.security.put_role(name=view, body={
                        'indices': [{
                            'names': view_indices,
                            'privileges': ["read"]
                        }]
                    })

            logger.info('Removed %s from %d views', dashboard_id, len(views))


def get_query_by_dashboard_id(dashboard_id):
    """Return query that filters by dashboard_id"""
    return {
        "query": {
            "bool": {
                "filter": {
                    "term": {
                        "dashboard_id": dashboard_id
                    }
                }
            }
        }
    }


def clean_fields(df):
    """Remove invalid characters from column names"""
    invalid_chars = ['.']
    invalid_cols = [col for col in df.columns if any(
        [char in col for char in invalid_chars])]
    for col in invalid_cols:
        found_chars = [char for char in invalid_chars if char in col]

        for char in found_chars:
            new_col = col.replace(char, '_')
            df.rename(columns={col: new_col}, inplace=True)


def clean_nans(record):
    """Delete any fields that contain nan values"""
    floats = [field for field in record if isinstance(record[field], float)]
    for field in floats:
        if np.isnan(record[field]):
            del record[field]
