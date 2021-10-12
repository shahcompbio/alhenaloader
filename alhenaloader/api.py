from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.exceptions import NotFoundError
import ssl
from elasticsearch.connection import create_ssl_context
import os
import numpy as np
import click

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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

    ANALYSIS_ENTRY_INDEX = "analyses"
    LABELS_INDEX = "metadata_labels"

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
        """Load individual recoElrd"""
        if not self.es.indices.exists(index):
            self.create_index(index, mapping=mapping)

        click.echo(f'Loading record with id {record_id}')
        self.es.index(index=index, id=record_id, body=record)

    def load_df(self, df, index_name, batch_size=int(1e5)):
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
            click.echo(
                f"Loading {len(records)} records. Total: {num_records} / {total_records} ({(num_records * 100 / total_records): .1f}%)")
        if total_records != num_records:
            raise ValueError(
                'mismatch in {num_records} records loaded to {total_records} total records')

    def load_records(self, records, index, mapping=None):
        """Load batch of records"""
        if not self.es.indices.exists(index):
            self.create_index(index, mapping=mapping)

        for success, info in helpers.parallel_bulk(self.es, records, index=index):
            if not success:
                click.secho('Doc failed in parallel loading', fg="red")
                click.echo(info)

    def create_index(self, index_name, mapping=None):
        click.echo(f'Creating index with name {index_name}')

        if mapping is None:
            mapping = DEFAULT_MAPPING

        self.es.indices.create(index=index_name,
                               body=mapping)

    def delete_index(self, index):
        if self.es.indices.exists(index):
            click.echo(f"Deleting index {index}")
            self.es.indices.delete(index=index, ignore=[400, 404])

    def delete_record_by_id(self, index, analysis_id):
        if self.es.indices.exists(index):

            try:
                click.echo(f"Deleting record with ID {analysis_id}")
                self.es.delete(index, analysis_id, refresh=True)

            except NotFoundError:
                return

    def delete_records_by_analysis_id(self, index, analysis_id):
        if self.es.indices.exists(index):

            click.echo(f"Deleting records from analysis {analysis_id}")
            query = get_query_by_analysis_id(analysis_id)
            self.es.delete_by_query(index=index, body=query, refresh=True)

    def delete_analysis_record(self, analysis_id):
        """Delete individual analysis record"""
        click.echo(f"Deleting analysis {analysis_id}")
        self.delete_records_by_analysis_id(
            self.ANALYSIS_ENTRY_INDEX, analysis_id)

    def is_loaded(self, analysis_id):
        """Return true if analysis with analysis_id exists"""
        try:
            self.es.get(self.ANALYSIS_ENTRY_INDEX, analysis_id)
            return True

        except NotFoundError:
            return False


    # Labels
    ## Each label is ID by their field name (like library_id, dashboard_id, etc)

    def initialize_labels(self):
        self.create_index(self.LABELS_INDEX)
        self.add_label("dashboard_id", "Dashboard ID")
        self.add_label("library_id", "Library ID")
        self.add_label("sample_id", "Sample ID")
        self.add_label("description", "Description")

    def get_labels(self):
        """Returns list of all labels"""
        response = self.es.search(index=self.LABELS_INDEX, body={"size": 10000})

        return [record['_source'] for record in response['hits']['hits']]

    def add_label(self, id, name=None):
        """Adds label to index"""

        record = {
            "id": id,
            "name": id if name is None else name
        }
        
        self.load_record(record, id, self.LABELS_INDEX)

    def get_missing_labels(self):
        """Returns list of all metadata fields that do not have labels"""

        labels = [record['id'] for record in self.get_labels()]

        fields_response = self.es.indices.get_mapping(self.ANALYSIS_ENTRY_INDEX)
        fields = list(fields_response['analyses']['mappings']['properties'].keys())

        labels.sort()
        fields.sort()

        diff = list(set(fields) - set(labels))

        return [field for field in diff if field not in ['dashboard_type', 'jira_id']]

    # Projects

    def get_projects(self):
        """Returns list of all project names"""
        response = self.es.security.get_role()
        projects = [response_key[:-len("_dashboardReader")] for response_key in response.keys(
        ) if response_key.endswith("_dashboardReader")]

        return projects

    def is_project_exist(self, project):
        """Returns true if project name exists"""
        project_name = f'{project}_dashboardReader'

        try:
            result = self.es.security.get_role(name=project_name)
            return project_name in result

        except NotFoundError:
            return False

    def verify_analyses_loaded(self, analyses):
        """Checks that all analyses are loaded"""
        unloaded_analyses = [
            analysis for analysis in analyses if not self.is_loaded(analysis)]

        assert len(
            unloaded_analyses) == 0, f"Analyses are not loaded: {unloaded_analyses}"

    def add_project(self, project, analyses=None):
        """Adds a new project"""
        project_name = f'{project}_dashboardReader'

        assert not self.is_project_exist(
            project_name), f'project with name {project} already exists'

        if analyses is None:
            analyses = []

        self.verify_analyses_loaded(analyses)

        self.es.security.put_role(name=project_name, body={'indices': [{
            'names': [self.ANALYSIS_ENTRY_INDEX] + analyses,
            'privileges': ["read"]
        }]})

        click.echo(f'Added new project: {project}')

    def add_analyses_to_project(self, project, analyses):
        """Add aa list of analysis IDs to a particular project"""
        assert self.is_project_exist(
            project), f'project with name {project} does not exist'

        self.verify_analyses_loaded(analyses)

        project_name = f'{project}_dashboardReader'
        project_data = self.es.security.get_role(name=project_name)

        project_indices = list(
            project_data[project_name]["indices"][0]["names"]) + analyses

        self.es.security.put_role(name=project_name, body={
            'indices': [{
                'names': list(set(project_indices)),
                'privileges': ["read"]
            }]
        })

        click.echo(f'Added {len(analyses)} analyses to project {project}')

    def add_analysis_to_projects(self, analysis, projects):
        """Add an analysis to all given projects"""
        assert self.is_loaded(
            analysis), f"Analysis with id {analysis} is not loaded, please load first"
        nonexistant_projects = [
            project for project in projects if not self.is_project_exist(project)]

        assert len(
            nonexistant_projects) == 0, f"projects do not exist: {nonexistant_projects}"

        for project in projects:
            self.add_analyses_to_project(project, [analysis])

    def remove_project(self, project):
        """Removes project"""
        project_name = f'{project}_dashboardReader'

        self.es.security.delete_role(project_name)
        click.echo(f'Removed project {project}')

    def remove_analysis_from_projects(self, analysis_id, projects=None):
        """Remove analysis from projects if specified, all projects if not"""
        if projects is None:
            click.echo("Fetching all projects")
            response = self.es.security.get_role()
            projects = [response_key for response_key in response.keys(
            ) if response_key.endswith("_dashboardReader")]

        else:
            projects = [f"{project}_dashboardReader" for project in projects]

        click.echo(
            f"Checking removal of {analysis_id} from {len(projects)} projects")

        for project in projects:
            response = self.es.security.get_role(name=project)
            project_data = response[project]

            project_indices = list(project_data["indices"][0]["names"])

            if analysis_id in project_indices:
                click.echo(f"Removing from {project}")

                project_indices.remove(analysis_id)

                self.es.security.put_role(name=project, body={
                    'indices': [{
                        'names': project_indices,
                        'privileges': ["read"]
                    }]
                })


def get_query_by_analysis_id(analysis_id):
    """Return query that filters by analysis_id"""
    return {
        "query": {
            "bool": {
                "filter": {
                    "term": {
                        "dashboard_id": analysis_id
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
