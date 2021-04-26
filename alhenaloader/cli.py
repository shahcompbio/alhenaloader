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
import logging
from typing import List
import click
from scgenome.loaders.qc import load_qc_data

from alhenaloader.api import ES
import alhenaloader.load
from .__init__ import __version__

LOGGING_LEVELS = {
    0: logging.NOTSET,
    1: logging.ERROR,
    2: logging.WARN,
    3: logging.INFO,
    4: logging.DEBUG,
}  #: a mapping of `verbose` option counts to logging levels


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
@click.option("--verbose", "-v", count=True, help="Enable verbose output.")
@click.option('--host', default='localhost', help='Hostname for Elasticsearch server')
@click.option('--port', default=9200, help='Port for Elasticsearch server')
@click.option('--id', help="ID of dashboard")
@pass_info
def cli(info: Info, verbose: int, host: str, port: int, id: str):
    """Run alhenaloader."""
    # Use the verbosity count to determine the logging level...
    if verbose > 0:
        logging.basicConfig(
            level=LOGGING_LEVELS[verbose]
            if verbose in LOGGING_LEVELS
            else logging.DEBUG
        )
        click.echo(
            click.style(
                f"Verbose logging is enabled. "
                f"(LEVEL={logging.getLogger().getEffectiveLevel()})",
                fg="yellow",
            )
        )
    info.verbose = verbose
    info.es = ES(host, port)
    info.id = id


@cli.command()
@pass_info
def clean(info: Info):
    """Delete indices/records associated with dashboard ID"""
    assert info.id is not None, "Please specify a dashboard ID"

    alhenaloader.load.clean_data(info.id, info.es)

    info.es.delete_record_by_id(
        info.es.DASHBOARD_ENTRY_INDEX, info.id)

    info.es.remove_dashboard_from_views(info.id)


@cli.command()
@click.option('--qc', help='Directory with qc results')
@click.option('--alignment', help='Directory with alignment results')
@click.option('--hmmcopy', help='Directory with hmmcopy results')
@click.option('--annotation', help='Directory with annotation results')
@click.option('--view', '-v', 'views', multiple=True, default=["DLP"], help="Views to load dashboard into")
@click.option('--library', required=True, help='Library ID of dashboard')
@click.option('--sample', required=True, help='Sample ID of dashboard')
@click.option('--description', required=True, help='Description of dashboard')
@click.option('--metadata', 'metadata', multiple=True, help='Additional metadata')
@pass_info
def load(info: Info, qc: str, alignment: str, hmmcopy: str, annotation: str, views: List[str], library: str, sample: str, description: str, metadata: List[str]):
    """Load records associated with dashboard ID in given directories"""
    assert info.id is not None, "Please specify a dashboard ID"

    is_all_dir = alignment is not None and hmmcopy is not None and annotation is not None
    assert qc is not None or is_all_dir, "Please provide a qc directory or all of annotation, hmmcopy, and alignment directories"

    if qc is not None:
        data = load_qc_data(qc)
    else:
        data = alhenaloader.load.load_qc_from_dirs(
            alignment, hmmcopy, annotation)

    alhenaloader.load.load_data(data, info.id, info.es)

    processed_metadata = {}
    for meta_str in metadata:
        [key, value] = meta_str.split(":")
        processed_metadata[key] = value
    alhenaloader.load.load_dashboard_entry(
        info.id, library, sample, description, processed_metadata, info.es)

    info.es.add_dashboard_to_views(info.id, list(views))


@cli.command()
@click.argument('view')
@click.option('--dashboard', '-d', 'dashboards', multiple=True, help="List of dashboards names")
@pass_info
def add_view(info: Info, view: str, dashboards: List[str]):
    """Add new view with name"""
    info.es.add_view(view, dashboards=list(dashboards))


@cli.command()
@click.argument('view')
@click.option('--dashboard', '-d', 'dashboards', multiple=True, help="List of dashboards names")
@pass_info
def add_dashboard_to_view(info: Info, view: str, dashboards: List[str]):
    """Add given dashboard IDs to view"""
    info.es.add_dashboards_to_view(view, list(dashboards))


@cli.command()
@click.argument('view')
@pass_info
def remove_view(info: Info, view):
    """Remove view"""
    info.es.remove_view(view)


@cli.command()
@pass_info
def list_views(info: Info):
    """List all views"""
    views = info.es.get_views()
    for view in views:
        click.echo(view)


@cli.command()
@pass_info
def initialize(info: Info):
    """Initalize database"""
    info.es.create_index(info.es.DASHBOARD_ENTRY_INDEX)
    info.es.add_view("DLP")


@cli.command()
def version():
    """Get the library version."""
    click.echo(click.style(f"{__version__}", bold=True))
