"""Publish entries to GitHub."""
import datetime
import os
import time
from pathlib import Path
from typing import Any, Dict

import diskcache
import github
import pandas as pd

from .. import config
from ..util.str import readable_list
from ._base import BasePublisher


class Publication:

    CURRENT_VERSION = 1

    def __init__(self, df_entries: pd.DataFrame):
        self.df_entries = df_entries
        self.date_utc = datetime.datetime.utcnow().date()
        self.version = self.CURRENT_VERSION

    @property
    def entries_csv(self) -> str:
        assert not self.df_entries.empty
        return self.df_entries.to_csv(index=False)

    @property
    def is_version_current(self) -> bool:
        """Return whether the instance version is the current version.

        This check can be relevant after restoring a pickled instance.
        """
        return self.version == self.CURRENT_VERSION


class Publisher(BasePublisher):
    """Publish a list of previously unpublished entries as a new file to GitHub."""

    def __init__(self):
        super().__init__(name=Path(__file__).stem)
        self._github = github.Github(os.environ["GITHUB_TOKEN"].strip())
        self._repo = self._github.get_repo(self.config)
        self._cache = diskcache.Cache(directory=config.DISKCACHE_PATH / f"{self.name.title()}{self.__class__.__name__}", timeout=2, size_limit=config.DISKCACHE_SIZE_LIMIT)

    def _publish(self, channel: str, df_entries: pd.DataFrame) -> Dict[str, Any]:
        assert not df_entries.empty
        pub = Publication(df_entries)
        path = f"{channel}/{pub.date_utc.strftime('%Y/%m%d')}.csv"  # Ref: https://strftime.org/
        new_feed_counts = readable_list([f"{count} {value}" for value, count in df_entries["feed"].value_counts().iteritems()])
        commit_message = f"Add {new_feed_counts} entries of {channel}"

        # Merge with day's history
        if (cached_pub := self._cache.get(channel)) and pub.is_version_current and pub.date_utc == cached_pub.date_utc:
            # Merge with disk cache
            pub.df_entries = pd.concat((cached_pub.df_entries, pub.df_entries))
            pub.sha = self._repo.update_file(path=path, message=commit_message, content=pub.entries_csv, sha=cached_pub.sha)["content"].sha
            self._cache[channel] = pub
        else:
            try:
                # Update content
                pub.sha = self._repo.get_contents(path=path)["content"].sha
            except github.GithubException.UnknownObjectException:
                # Create content
                pub.sha = self._repo.create_file(path=path, message=commit_message, content=pub.entries_csv)["content"].sha
            self._cache[channel] = pub

        content = df_entries.to_csv(index=False)
        self._repo.create_file(path=path, message=commit_message, content=content)
        return {
            "path": path,
            # "content_len": len(content),
            "rate_remaining": self._github.rate_limiting[0],
            # "rate_limit": self._github.rate_limiting[1],  # Always 5000.
            "rate_reset": datetime.timedelta(seconds=round(self._github.rate_limiting_resettime - time.time())),
        }
