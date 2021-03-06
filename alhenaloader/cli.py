#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This is the entry point for the command-line interface (CLI) application.

It can be used as a handy facility for running the task from a command line.

.. note::

    To learn more about Click visit the
    `project website <http://click.pocoo.org/5/>`_.  There is also a very
    helpful `tutorial video <https://www.youtube.com/watch?v=kNke39OZ2k0>`_.

    To learn more about running Luigi, visit the Luigi project's
    `Read-The-Docs <http://luigi.readthedocs.io/en/stable/>`_ page.

.. currentmodule:: alhenaloader.cli
.. moduleauthor:: Samantha Leung <leungs1@mskcc.org>
"""
from typing import List
import click
from scgenome.loaders.qc import load_qc_results

from alhenaloader.api import ES
import alhenaloader.load
from .__init__ import __version__


class Info(object):
    """An information object to pass data between CLI functions."""

    def __init__(self):  # Note: This object must have an empty constructor.
        """Create a new instance."""
        self.verbose: int = 0


# pass_info is a decorator for functions that pass 'Info' objects.
#: pylint: disable=invalid-name
pass_info = click.make_pass_decorator(Info, ensure=True)


# Change the options to below to suit the actual options for your task (or
# tasks).
@click.group(chain=True)
@click.option('--host', default='localhost', help='Hostname for Elasticsearch server')
@click.option('--port', default=9200, help='Port for Elasticsearch server')
@click.option('--id', help="ID of analysis")
@pass_info
def cli(info: Info, host: str, port: int, id: str):
    """Run alhenaloader."""

    info.es = ES(host, port)
    info.id = id


@cli.command()
@pass_info
def clean(info: Info):
    """Delete indices/records associated with analysis ID"""
    if info.id is None:
        click.secho("Please specify a analysis ID", fg="yellow")
        return

    alhenaloader.load.clean_analysis(info.id, info.es)


@cli.command()
@click.option('--qc', help='Directory with qc results')
@click.option('--alignment', help='Directory with alignment results')
@click.option('--hmmcopy', help='Directory with hmmcopy results')
@click.option('--annotation', help='Directory with annotation results')
@click.option('--project', '-p', 'projects', multiple=True, default=["DLP"], help="Projects to load analysis into")
@click.option('--library', required=True, help='Library ID of analysis')
@click.option('--sample', required=True, help='Sample ID of analysis')
@click.option('--description', required=True, help='Description of analysis')
@click.option('--metadata', 'metadata', multiple=True, help='Additional metadata')
@pass_info
def load(info: Info, qc: str, alignment: str, hmmcopy: str, annotation: str, projects: List[str], library: str, sample: str, description: str, metadata: List[str]):
    """Load records associated with analysis ID in given directories"""
    if info.id is None:
        click.secho("Please specify a analysis ID", fg="yellow")
        return

    is_all_dir = alignment is not None and hmmcopy is not None and annotation is not None

    if qc is None and not is_all_dir:
        click.secho(
            "Please provide a qc directory or all of annotation, hmmcopy, and alignment directories")
        return

    if qc is not None:
        data = load_qc_results(qc, qc, qc)
    else:
        data = load_qc_results(alignment, hmmcopy, annotation)

    processed_metadata = {}
    for meta_str in metadata:
        [key, value] = meta_str.split(":")
        processed_metadata[key] = value
    analysis_record = alhenaloader.load.process_analysis_entry(
        info.id, library, sample, description, processed_metadata)

    alhenaloader.load.load_analysis(info.id, data, analysis_record, list(projects), info.es)
    

@cli.command()
@click.argument('project')
@click.option('--analysis', '-a', 'analyses', multiple=True, help="List of analysis IDs to add to project")
@pass_info
def add_project(info: Info, project: str, analyses: List[str]):
    """Add new project with name"""
    info.es.add_project(project, analyses=list(analyses))


@cli.command()
@click.argument('project')
@click.option('--analysis', '-a', 'analyses', multiple=True, help="List of analysis IDs to add to project")
@pass_info
def add_analyses_to_project(info: Info, project: str, analyses: List[str]):
    """Add given analysis IDs to view"""
    info.es.add_analyses_to_project(project, list(analyses))


@cli.command()
@click.argument('project')
@pass_info
def remove_project(info: Info, project):
    """Remove project"""
    info.es.remove_project(project)


@cli.command()
@pass_info
def list_project(info: Info):
    """List all projects"""
    projects = info.es.get_projects()
    for project in projects:
        click.echo(project)


@cli.command()
@pass_info
def update_to_v105(info: Info):

    ## need to check whether v1.0.4 is applied
    ## one, needs labels index

    ## two, need to ensure all analyses have dashboard_id


    ## For v1.0.5, need to add cell_count
    ## so first, need to check that each analysis we have metadata for has data as well
    if not info.es.es.indices.exists(info.es.LABELS_INDEX):
        print('Bleh')
        # info.es.initialize_labels()
        # info.es.verify_analyses_v104()
    print('Test')
    
    info.es.add_cell_count()


@cli.command()
@click.option('--delete', is_flag=True)
@pass_info
def find_dangling_analyses(info: Info, delete: bool):
    ## find analyses with metadata but no data
    info.es.verify_data(delete)

    

@cli.command()
@pass_info
def initialize(info: Info):
    """Initalize database"""
    info.es.create_index(info.es.ANALYSIS_ENTRY_INDEX)
    info.es.add_project("DLP")
    info.es.initialize_labels()


@cli.command()
def version():
    """Get the library version."""
    click.echo(click.style(f"{__version__}", bold=True))
