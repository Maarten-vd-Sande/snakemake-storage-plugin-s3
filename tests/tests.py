import asyncio
from collections import Counter
from typing import List, Optional, Type
import uuid
from snakemake.io import IOCache
from snakemake_interface_storage_plugins.tests import TestStorageBase
from snakemake_interface_storage_plugins.storage_provider import StorageProviderBase
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_interface_executor_plugins.settings import ExecutorSettingsBase
from snakemake_interface_executor_plugins.registry import ExecutorPluginRegistry
from snakemake.executors import local as local_executor
from snakemake_storage_plugin_s3 import StorageProvider, StorageProviderSettings
from snakemake.common.tests import TestWorkflowsMinioPlayStorageBase
import snakemake.common.tests
import snakemake.settings.types


def count_requests(provider):
    """Count the HTTP requests sent by the provider's S3 client, per operation."""
    counts = Counter()

    def count(event_name, **kwargs):
        counts[event_name.rsplit(".", 1)[-1]] += 1

    provider.s3c.meta.client.meta.events.register("before-send.s3", count)
    return counts


def upload(provider, bucket, keys):
    """Store a small file under each key and return the storage objects."""
    objs = {key: provider.object(f"s3://{bucket}/{key}") for key in keys}
    for obj in objs.values():
        obj.local_path().parent.mkdir(parents=True, exist_ok=True)
        obj.local_path().write_text("test")
        obj.store_object()
    return objs


def delete_bucket(provider, bucket):
    try:
        s3bucket = provider.s3c.Bucket(bucket)
        s3bucket.objects.all().delete()
        s3bucket.delete()
    except Exception:
        pass


class TestStorageNoSettings(TestStorageBase):
    __test__ = True
    retrieve_only = False
    files_only = False

    def get_query(self, tmp_path) -> str:
        return "s3://snakemake-test-bucket/testdir1/testdir2/test-file.txt"

    def get_query_not_existing(self, tmp_path) -> str:
        bucket = uuid.uuid4().hex
        key = uuid.uuid4().hex
        return f"s3://{bucket}/{key}"

    def get_storage_provider_cls(self) -> Type[StorageProviderBase]:
        # Return the StorageProvider class of this plugin
        return StorageProvider

    def get_storage_provider_settings(self) -> Optional[StorageProviderSettingsBase]:
        # instantiate StorageProviderSettings of this plugin as appropriate
        return StorageProviderSettings(
            endpoint_url="http://127.0.0.1:9000",
            access_key="minio",
            secret_key="minio123",
        )

    def get_example_args(self) -> List[str]:
        return []

    def test_inventory_requests(self, tmp_path):
        provider = self._get_provider(tmp_path)
        sent = count_requests(provider)
        bucket = f"snakemake-test-{uuid.uuid4().hex}"
        keys = ["d1/a", "d1/b", "d1/sub/c", "d2/a", "d2/b", "d1_old/a", "top"]
        try:
            objs = upload(provider, bucket, keys)
            # the bucket is checked (and created) once, not before every upload
            assert dict(sent) == {"HeadBucket": 1, "CreateBucket": 1, "PutObject": 7}

            sent.clear()
            cache = IOCache(max_wait_time=10)
            asyncio.run(objs["d1/a"].inventory(cache))
            asyncio.run(objs["d2/a"].inventory(cache))
            # one listing per directory
            assert dict(sent) == {"ListObjects": 2}
            for key in ["d1/a", "d1/b", "d1/sub/c", "d2/a", "d2/b"]:
                cache_key = objs[key].cache_key()
                assert cache.exists_in_storage[cache_key]
                assert cache_key in cache.mtime
                assert cache.size[cache_key] == len("test")
            # a sibling directory sharing the name prefix "d1" is not listed
            assert objs["d1_old/a"].cache_key() not in cache.exists_in_storage

            sent.clear()
            # covered by the listing of d1, which is recursive
            asyncio.run(objs["d1/b"].inventory(cache))
            asyncio.run(objs["d1/sub/c"].inventory(cache))
            # top-level keys never trigger a listing of the whole bucket
            asyncio.run(objs["top"].inventory(cache))
            assert not sent
        finally:
            delete_bucket(provider, bucket)

    def test_lookups_use_folder_listings(self, tmp_path):
        provider = self._get_provider(tmp_path)
        sent = count_requests(provider)
        bucket = f"snakemake-test-{uuid.uuid4().hex}"
        keys = ["d1/a", "d1/sub/b", "d2/a", "d2/b", "top"]
        try:
            objs = upload(provider, bucket, keys)
            cache = IOCache(max_wait_time=10)
            # lists d1/ and lets the plugin know the cache
            asyncio.run(objs["d1/a"].inventory(cache))

            sent.clear()
            # the first lookup in d2/ lists that folder ...
            assert objs["d2/a"].exists(), 1
            # ... so its sibling is answered from the listing
            assert objs["d2/b"].exists(), 2
            assert objs["d2/b"].mtime() > 0, 3
            assert objs["d2/b"].size() == len("test"), 4
            # the listing of d1/ also tells which files and folders exist
            assert not provider.object(f"s3://{bucket}/d1/missing").exists(), 5
            assert dict(sent) == {"ListObjects": 1}, 6

            sent.clear()
            # top-level keys are never listed (see #24), so they are looked up
            assert objs["top"].exists(), 7
            # so are objects with a custom local path, which have no cache key
            custom = provider.object(f"s3://{bucket}/d1/a")
            custom.set_local_path(tmp_path / "custom")
            assert custom.exists(), 8
            assert dict(sent) == {"HeadObject": 2}, 9

            sent.clear()
            # once Snakemake deactivates the cache, lookups ask S3 again
            cache.deactivate()
            assert objs["d2/b"].exists(), 10
            assert dict(sent) == {"HeadObject": 1}, 11
        finally:
            delete_bucket(provider, bucket)


registry = ExecutorPluginRegistry()
registry.register_plugin("local", local_executor)


class TestWorkflows(TestWorkflowsMinioPlayStorageBase):
    __test__ = True

    def get_executor(self) -> str:
        return "local"

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        return None

    def get_assume_shared_fs(self) -> bool:
        return True

    def get_remote_execution_settings(
        self,
    ) -> snakemake.settings.types.RemoteExecutionSettings:
        return snakemake.settings.types.RemoteExecutionSettings()
