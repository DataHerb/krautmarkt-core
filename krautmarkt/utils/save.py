import json
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def save_json(dic, path):
    """
    save_json saves the dictionary of metadata as json file
    """

    logger.info(f"Will save {dic} to {path}")
    with open(path, "w+") as fp:
        json.dump(dic, fp)

    logger.info(f"Saved {dic} to {path}")


def markdown_lists(dic_lists, name):

    if dic_lists:
        md_hugo = f"{name}:"
        for l in dic_lists:
            md_hugo = md_hugo + f'\n  - "{l}"'
        md_hugo = md_hugo + "\n"
    else:
        md_hugo = ""

    return md_hugo


def save_markdown(dic, path, endpoint=None):
    """
    save_markdown generates a markdown file
    """

    logger.info(f"Will save {dic} to {path}")

    metadata_title = dic.get("profile")
    metadata_description = dic.get("description")
    metadata_keywords = dic.get("keywords")
    metadata_categories = dic.get("categories", ["MISC"])

    keywords_hugo = markdown_lists(metadata_keywords, "keywords")
    categories_hugo = markdown_lists(metadata_categories, "categories")

    if endpoint is None:
        endpoint = "metadata.json"

    metadata_hugo = '---\ntitle: "{}"\nendpoint: {}\n'.format(metadata_title, endpoint)
    if metadata_description:
        metadata_hugo = metadata_hugo + f'description: "{metadata_description}"\n'
    if keywords_hugo:
        metadata_hugo = metadata_hugo + keywords_hugo
    if categories_hugo:
        metadata_hugo = metadata_hugo + categories_hugo

    # end the metadata region
    metadata_hugo = metadata_hugo + "---"

    with open(path, "w") as fp:
        fp.write(metadata_hugo)

    logger.info(f"Saved {dic} to {path}")


if __name__ == "__main__":

    pass
