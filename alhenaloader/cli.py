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
import click
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
@click.option('--id', help="ID of analysis", required=True)
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
    info.id = id
    info.host = host
    info.port = port


@cli.command()
@pass_info
def clean(info: Info):
    """Delete indices/records associated with dashboard ID"""
    click.echo(f"Cleaning {info.id}")


@cli.command()
@pass_info
def load(info: Info):
    """
    Load records under new indices named after dashboard ID

    Data is contained either in one file directory (--qc) or 
    split amongst three separate ones (--hmmcopy, --alignment, --annotation)

    """
    click.echo(f"Loading {info.id}")


@cli.command()
@pass_info
def add_view(info: Info):
    """Add new view with name"""
    click.echo("Add view")


@cli.command()
@pass_info
def add_dashboard_to_view(info: Info):
    """Add dashboard ID to view"""
    click.echo("Add dashboard to a view")


@cli.command()
@pass_info
def list_views(info: Info):
    """List all views"""
    click.echo("List all views")


@cli.command()
@pass_info
def initialize(info: Info):
    """Initalize database"""
    click.echo("Initialize database")


@cli.command()
def version():
    """Get the library version."""
    click.echo(click.style(f"{__version__}", bold=True))
