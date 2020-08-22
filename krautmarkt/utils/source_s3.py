import json
import s3fs
from loguru import logger
from krautmarkt.utils.models import SourceModel


def SourceS3(SourceModel):
    """
    SourceS3 take the data folders in S3 and generate the corresponding index
    """
    def __init__(self, path_to_datasets: str):
        self.path_to_datasets = path_to_datasets
        self.metas = []

    def fetch_metadata(self) -> list:
        """
        fetch_metadata fetches all the metadata from the remote service
        """

        # initiate a filesystem service
        fs = s3fs.S3FileSystem(anon=False)

        # get all datasetfiles
        datasets = fs.ls(self.path_to_datasets, detail=True)

        metas = []

        for ds in datasets:
            # get key
            ds_meta_content = self._fetch_one_metadata(fs, ds)
            if ds_meta_content:
                metas.append(ds_meta_content)

        self.metas = metas

        return metas

    def _fetch_one_metadata(self, fs, ds):
        """
        _fetch_one_metadata fetches a metadata file from a given S3 folder

        :param fs: S3 filesystem object
        :type fs: s3fs.S3FileSystem
        :param ds: dataset dictionary from S3 filesystem ls results
        :type ds: dict
        :return: metadata content
        :rtype: dict
        """

        ds_key = ds.get("Key")
        logger.info(f"Processing {ds_key}")
        # content of the file
        ds_content = fs.ls(ds_key)
        logger.info(f"list of files in dataset: {ds_content}")

        # verify metadata
        ds_meta_remote = "/".join([ds_key, "metadata.json"])
        logger.info(f"Check and validate metadata: {ds_meta_remote}")
        if not fs.exists(ds_meta_remote):
            logger.warning(f"metadata {ds_meta_remote} does not exist!")
            ds_meta_content = None
        else:
            logger.info("Metadata exists!")
            with fs.open("s3://" + ds_meta_remote, "r") as f:
                ds_meta_content = json.load(f)

            logger.info(f"metadata: {ds_meta_content}")
            # pkg = datapackage.Package(ds_meta_content, unsafe=True)
            # logger.info(f'descriptor: {pkg}')

        return ds_meta_content


def fetch_metadata(path_to_datasets):

    # initiate a filesystem service
    fs = s3fs.S3FileSystem(anon=False)

    # get all datasetfiles
    datasets = fs.ls(path_to_datasets, detail=True)

    metas = []

    for ds in datasets:
        # get key
        ds_key = ds.get("Key")
        logger.info(f"Processing {ds_key}")
        # content of the file
        ds_content = fs.ls(ds_key)
        logger.info(f"list of files in dataset: {ds_content}")

        # verify metadata
        ds_meta_remote = "/".join([ds_key, "metadata.json"])
        logger.info(f"Check and validate metadata: {ds_meta_remote}")
        if not fs.exists(ds_meta_remote):
            logger.warning(f"metadata {ds_meta_remote} does not exist!")
        else:
            logger.info("Metadata exists!")
            with fs.open("s3://" + ds_meta_remote, "r") as f:
                ds_meta_content = json.load(f)

            logger.info(f"metadata: {ds_meta_content}")
            # pkg = datapackage.Package(ds_meta_content, unsafe=True)
            # logger.info(f'descriptor: {pkg}')
            metas.append(ds_meta_content)

    return metas


if __name__ == "__main__":

    default_path_to_datasets = "lma/krautmarkt"
    ms = SourceS3(path_to_datasets=default_path_to_datasets)
    ms.fetch_metadata()

    print(ms.metas)

    pass
