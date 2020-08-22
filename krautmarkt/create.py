import json
import os
from slugify import slugify
from krautmarkt.utils.source_s3 import fetch_metadata as _fetch_metadata
from krautmarkt.utils.save import save_json as _save_json
from krautmarkt.utils.save import save_markdown as _save_markdown
import logging
import click

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def generate_datasets(
    remote_bucket,
    project=None,
    json_path=None,
    md_path=None,
    has_md=None,
    endpoint=None,
):
    if project is None:
        project = "."

    if endpoint is None:
        endpoint = "metadata.json"

    if json_path is None:
        json_path = os.path.join("data", "markt")
    if md_path is None:
        md_path = os.path.join("content", "markt")

    # attach working directory to all paths
    json_path = os.path.join(project, json_path)
    md_path = os.path.join(project, md_path)

    # create folders
    try:
        os.makedirs(md_path)
        logger.info(f"Created {md_path}")
    except FileExistsError as err:
        logger.info(f"{md_path} already exists!")

    if has_md is None:
        has_md = True

    ms = _fetch_metadata(remote_bucket)

    for m in ms:
        m_profile = slugify(m.get("profile"))
        m_json_folder = os.path.join(json_path, f"{m_profile}")

        try:
            os.makedirs(m_json_folder)
            logger.info(f"Created {md_path}")
        except FileExistsError as err:
            logger.info(f"{md_path} already exists!")

        m_json_path = os.path.join(m_json_folder, "metadata.json")
        _save_json(m, m_json_path)

        if has_md:
            md_path = os.path.join(md_path, f"{m_profile}.md")
            # generate markdown files
            _save_markdown(m, md_path, endpoint=endpoint)


@click.group(invoke_without_command=True)
@click.pass_context
def krautmarkt(ctx):
    if ctx.invoked_subcommand is None:
        click.echo("Hello {}".format(os.environ.get("USER", "")))
        click.echo("Welcome to Krautmarkt, your Kraut management system.")
    else:
        click.echo(f"Loading Service: {ctx.invoked_subcommand}")


@krautmarkt.command()
@click.option("--remote", "-r", type=str, required=True)
@click.option(
    "--project", "-p", type=click.Path(), required=True, help="Directory of the project"
)
def create(remote, project):
    if not os.path.exists(project):
        raise Exception(f"path {project} does not exist!")

    if not isinstance(remote, str):
        raise TypeError(f"remote option has type {type(remote)} where str is required!")

    generate_datasets(remote, project=project)


if __name__ == "__main__":

    default_path_to_datasets = "abc/krautmarkt"
    generate_datasets(default_path_to_datasets)

    pass
