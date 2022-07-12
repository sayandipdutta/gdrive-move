# HACK: Try to copy in separate process, and communicate to update progress.
# PERF: Check which functions take the longest.
# PERF: Try async or threading/multiprocessing.
# OPTIMIZE: Build a complete tree, and reuse when needed.
# OPTIMIZE: Use cache.
# FIX: Write tests.
# TODO: Init git.
# TODO: Add logging.
# TODO: Add docstring.


from functools import cache
import json
import os
from pathlib import Path
from signal import SIGINT
import shlex
import subprocess
import time
from typing import Generator, Iterable, Literal, Optional, cast, overload

from getfilelistpy import getfilelist   # type: ignore
from google.auth.transport.requests import Request    # type: ignore
from google.oauth2.credentials import Credentials    # type: ignore
from google_auth_oauthlib.flow import InstalledAppFlow    # type: ignore
from googleapiclient.discovery import Resource, build    # type: ignore
from googleapiclient.errors import HttpError    # type: ignore
from pydantic import ValidationError
from rich.console import Console
from rich.progress import TaskID
# from rich.panel import Panel

from . import TOKEN, CREDS
from .datatypes import (
    CopyStats,
    Cluster,
    File,
    Folder,
    FileType,
    FolderType,
    ItemID,
    FOLDER_MIME_TYPE,
    Item,
    SupportRich,
    folder_to_id,
    format_size
)


# NOTE: If modifying these scopes, delete the file token.json.
SCOPES = [
    'https://www.googleapis.com/auth/drive.metadata.readonly',
    'https://www.googleapis.com/auth/drive'
]


class DriveService(SupportRich):
    max_search_pages: int = 50
    page_size: int = 100

    def __init__(self, *, console: Optional[Console] = None):
        if console is None:
            super().__init__()
        else:
            super().__init__(console=console)
        self._creds: Credentials = self.get_creds()
        self._service: Resource = build("drive", "v3", credentials=self.creds)

    def __enter__(self) -> 'DriveService':
        self.progress.start()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.service.close()
        self.progress.stop()

        if isinstance(exc_value, HttpError):
            self.progress.log(
                "[red]ERROR:[/red] While processing request.",
                exc_type, exc_value, exc_tb,
                log_locals=True
            )
            return True

    @property
    def creds(self):
        return self._creds

    @property
    def service(self):
        return self._service

    def get_creds(self) -> Credentials:
        """
        Check for valid credentials, and generate token.
        """

        assert isinstance(TOKEN, str) and isinstance(CREDS, str), \
            "Must Provide TOKEN and CREDS path."

        token = Path(TOKEN).expanduser()
        existing_creds = Path(CREDS).expanduser()

        creds = None

        # NOTE: token.json stores the user's access and refresh tokens.
        if token.exists():
            creds = Credentials.from_authorized_user_file(token, SCOPES)

        # if token not found, or token not valid, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    existing_creds, SCOPES
                )
                creds = flow.run_local_server(port=0)
            with open(token, 'w') as tk:
                tk.write(creds.to_json())

        return creds

    @overload
    def list_dir(
        self,
        folder_id: str,
        *,
        log: Optional[bool] = False,
        files_only: Optional[Literal[False]] = False,
        return_count: Optional[Literal[False]] = False,
    ) -> list[Item]:
        """Return items from the given folder. Only top-level items."""
        ...

    @overload
    def list_dir(
        self,
        folder_id: str,
        *,
        files_only: Literal[True],
        log: Optional[bool] = False,
        return_count: Optional[Literal[False]] = False,
    ) -> list[File]:
        """Return all files from a given forlder. Including sub-folders'."""
        ...

    @overload
    def list_dir(
        self,
        folder_id: str,
        *,
        return_count: Literal[True],
        files_only: Optional[Literal[False]] = False,
        log: Optional[bool] = False,
    ) -> tuple[list[Item], int]:
        """Return top-level items from folder, with count."""
        ...

    @overload
    def list_dir(
        self,
        folder_id: str,
        *,
        files_only: Literal[True],
        return_count: Literal[True],
        log: Optional[bool] = False,
    ) -> tuple[list[File], int]:
        """Return all files from folder, with count."""
        ...

    def list_dir(
        self,
        folder_id: str,
        *,
        files_only: Optional[bool] = False,
        return_count: Optional[bool] = False,
        log: Optional[bool] = False
    ) -> (
        list[Item] | list[File] |
        tuple[list[Item], int] | tuple[list[File], int]
    ):
        # TODO: Implement lazy loading.
        """
        Get contents of a folder.
        """
        items: list[Item] = []

        if files_only:
            resource = {
                "service_account": self.creds,
                "id": folder_id,
                "fields": "files(id, name, mimeType, size, parents)",
            }
            self.progress.log("Started searching for all files.")
            dir_listing_task = self.progress.add_task(
                "[blue]Listing files in dir", total=None)
            result = getfilelist.GetFileList(resource)
            self.progress.update(dir_listing_task, total=1, completed=1)
            self.progress.log(f"{result['totalNumberOfFiles']} files found.")
            items_f = [File(**item) for batch in result['fileList']
                       for item in batch['files']]
            if return_count:
                return items_f, cast(int, result['totalNumberOfFiles'])
            return items_f

        dir_listing_task = None
        if log:
            self.progress.log("Started searching for top-level files/folder.")
            dir_listing_task = self.progress.add_task(
                "[blue]Listing files in dir", total=None)

        try:
            page_token = None
            search_pages = 0
            results = []
            while search_pages < self.max_search_pages:
                response = self.service.files().list(
                    q=f"'{folder_id}' in parents",
                    spaces='drive',
                    corpora='allDrives',
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                    pageToken=page_token,
                    pageSize=self.page_size,
                    fields=(
                        "nextPageToken, "
                        "files(id, name, mimeType, size, parents)"
                    ),
                ).execute()
                results.extend(response.get('files', []))
                page_token = response.get('nextPageToken')
                if page_token is None:
                    break
                search_pages += 1
        except HttpError as err:
            self.progress.log("[bold red]ERROR:[/bold red]",
                              "While listing directory.", err, log_locals=True)
            if log:
                if dir_listing_task is not None:
                    self.progress.stop_task(dir_listing_task)

            if return_count:
                return items, len(items)
            return items

        if log:
            if dir_listing_task is not None:
                self.progress.update(dir_listing_task, total=1, completed=1)
            self.progress.log(f"{len(results)} items found.")

        items_ = [categorize(item) for item in results]
        if return_count:
            return items_, len(items_)
        return items_

    def make_cluster(
        self,
        items: Iterable[Item],
        *,
        upper_limit: int,
        max_clusters: int = 1,
        exclude: set[str] = set()
    ) -> Generator[Cluster[Item], None, None]:
        # TODO: Make a Cluster class to store cluster related info.
        if not isinstance(max_clusters, int) or max_clusters < 1:
            error = ValueError(
                f"While clustering. Invalid value: {max_clusters=}",
            )
            self.progress.log("ERROR: ", error, log_locals=True)
            raise error

        size = 0
        item_count = 0
        nclusters = 0
        cluster: list[Item] = []
        clustering_task = self.progress.add_task(
            "Clustering",
            total=upper_limit
        )
        for item in items:
            if item.name in exclude:
                continue
            item_size = size_on_disk(item)
            if size + item_size > upper_limit:
                self.progress.update(
                    clustering_task,
                    completed=size,
                    total=size
                )
                yield Cluster(cluster[:], size, item_count)
                nclusters += 1
                if nclusters >= max_clusters:
                    return None
                self.progress.update(
                    clustering_task,
                    completed=0,
                    total=upper_limit
                )
                size = 0
                item_count = 0
                cluster = []
            self.progress.advance(clustering_task, advance=item_size)
            item_count += 1
            cluster.append(item)
            size += item_size
        yield Cluster(cluster[:], size, item_count)
        self.progress.update(clustering_task, completed=size, total=size)

    @folder_to_id
    def move(
        self,
        item: Item | Cluster[Item],
        *,
        destination: ItemID,
    ):
        if not isinstance(item, File | Folder):
            total = len(list(item))
            moving_task = self.progress.add_task(
                "[magenta]Moving files",
                total=total
            )

            for each in item:
                self.move(each, destination=destination)
                self.progress.advance(moving_task, advance=1)
            self.progress.log(f"Total top-level folders moved: {total}")

        else:
            self.progress.log(f"Moving {item.name} to {destination}")
            previous_parents = ", ".join(item.parents)
            try:
                _ = self.service.files().update(
                    fileId=item.id,
                    addParents=destination,
                    removeParents=previous_parents,
                    fields='id, parents'
                ).execute()
            except HttpError as err:
                self.progress.log("ERROR: occurred while moving.", err,
                                  log_locals=True)
            except TimeoutError as err:
                self.progress.log("ERROR: occurred while moving.", err,
                                  log_locals=True)

    @folder_to_id
    def copy(
        self,
        source: ItemID,
        *,
        destination: ItemID,
        dest_path: str,
        port: str = "5572",
        size_hint: int = None,
        timeout: int = 900
    ):
        assert str(port).isnumeric(), "port must be an integer in string form."
        copy_task = self.progress.add_task(
            "Copying",
            total=size_hint,
            show_speed=True
        )
        # source_size = total_size(source)
        # self.progress.log(f"Total size of items present in source:
        # {source_size}")
        command = [
            "python3", "rclone_sa_magic.py",
            "-s", str(source),
            "-d", str(destination),
            "-dp", str(dest_path),
            "-b", "1",
            "-e", "600",
            "-p", port,
            "--disable_list_r"
        ]
        cwd = Path('~/github/BGFA_rclone/AutoRclone/').expanduser()
        rc_cmd = shlex.split(f'rclone rc --rc-addr="localhost:{port}" core/stats')
        start = time.perf_counter()
        size_bytes_done = 0
        total_done = 0
        prev_done = 0
        no_download = 0
        with open(cwd.parent / 'internal' / 'autorclone.log', 'w+', encoding='utf-8', buffering=1) as fh:
            with subprocess.Popen(
                command,
                cwd=cwd,
                stdout=fh,
                stderr=subprocess.STDOUT,
                encoding='utf-8'
            ) as proc:
                time.sleep(10)
                while proc.poll() is None:
                    try:
                        # breakpoint()
                        result = subprocess.run(
                            rc_cmd,
                            capture_output=True,
                            check=True,
                            encoding='utf-8',
                            cwd=cwd
                        )
                    except subprocess.CalledProcessError as error:
                        self.progress.log(
                            "[red]ERROR:[/red] while checking rclone stats",
                            error, log_locals=True
                        )
                        if time.perf_counter() - start > timeout:
                            self.progress.update(copy_task, total=1, completed=1)
                            os.kill(proc.pid, SIGINT)
                            self.progress.log(f"[red]Timed Out[/red]: {timeout=}")
                            break
                        continue
                    except FileNotFoundError:
                        if time.perf_counter() - start > timeout:
                            self.progress.update(copy_task, total=1, completed=1)
                            os.kill(proc.pid, SIGINT)
                            self.progress.log(f"[red]Timed Out[/red]: {timeout=}")
                            break
                        continue
                    response_processed = result.stdout.replace('\0', '')
                    response_processed_json = json.loads(response_processed)
                    size_bytes_done = int(response_processed_json['bytes'])
                    new_done = size_bytes_done - prev_done
                    if new_done < 0:
                        new_done = size_bytes_done
                    total_done += new_done
                    if prev_done == size_bytes_done:
                        no_download += 1
                    else:
                        no_download = 0
                    if no_download >= 300:
                        self.progress.log(
                            f"No download for {no_download} times.",
                        )
                        os.kill(proc.pid, SIGINT)
                        break
                    self.progress.update(
                        copy_task,
                        completed=total_done,
                    )
                    prev_done = size_bytes_done
        self.progress.log(
            "[bold green]COPY:[/bold green] copied -> "
            f"{format_size(size_bytes_done)}"
        )
        self.progress.update(copy_task, total=total_done,
                             completed=total_done)

    def delete(self, item: ItemID):
        try:
            self.service.files().delete(fileId=item).execute()
        except HttpError as err:
            self.progress.log(
                "[bold red]ERROR:[/bold red] occurred while deleting.",
                err,
                log_locals=True
            )

    @folder_to_id
    def create_folder(
        self,
        name: str,
        *,
        destination: ItemID
    ) -> Folder:
        file_metadata = {
            'name': name,
            'title': name,
            'mimeType': FOLDER_MIME_TYPE,
            'parents': [destination],
        }
        try:
            item = self.service.files().create(
                body=file_metadata,
                fields="id, name, mimeType, size, parents"
            ).execute()
        except HttpError as err:
            self.progress.log("[red]ERROR: While creating folder.", err)
            raise

        folder = Folder(**item)
        self.progress.log("New folder created", folder)
        return folder

    def search(
        self,
        query: str,
        **kwargs: ItemID
    ) -> Generator[Item, None, None]:
        query = query
        try:
            page_token = None
            while True:
                response = self.service.files().list(
                    q=query,
                    spaces='drive',
                    corpora='drive',
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                    fields=(
                        'nextPageToken, '
                        'files(id, name, mimeType, size, parents)'
                    ),
                    pageToken=page_token,
                    **kwargs
                ).execute()
                yield from (
                    categorize(item) for item in response.get('files', [])
                )
                page_token = response.get('nextPageToken', None)
                if page_token is None:
                    break

        except HttpError as err:
            self.progress.log("[bold red]ERROR:[/bold red]",
                              err, log_locals=True)
            return None

    def search_by_id(self, id: str) -> Item:
        # TODO: merge with search
        item = self.service.files().get(
            fileId=id,
            supportsAllDrives=True,
            supportsTeamDrives=True,
            fields="id, name, mimeType, size, parents"
        ).execute()
        return categorize(item)

    def _get_files_from_parent(
        self,
        source: ItemID,
    ) -> list[tuple[File, Folder]]:
        # TODO: merge with list_dir
        items = []
        files, total = self.list_dir(
            source, files_only=True, return_count=True
        )

        listing_task = self.progress.add_task(
            "[yellow]Gathering parents",
            total=total
        )
        hack = False
        for item in files:
            self.progress.advance(listing_task, advance=1)
            # HACK: DON'T NEED PARENT IF WE ARE NOT CHECKING IN REVIEW
            if not hack:
                parent = self.search_by_id(item.parents[0])
                assert isinstance(parent, Folder), "Parent must be a Folder"
                hack = True
            items.append((item, parent))
        return items

    @folder_to_id
    def update_permission_recursively(self, folder_id: ItemID, total: int=None):
        recursive_task = self.progress.add_task(
            "[magenta]Granting permissions recursively", total=total)
        self._permission_helper(folder_id)
        self._recursive_permission_helper(folder_id, progress=recursive_task)
        self.progress.update(recursive_task, total=total,
                             completed=total, visible=False)

    def _recursive_permission_helper(self, folder_id: str, progress: TaskID = None):
        items = self.service.files().list(
            q=f"'{folder_id}' in parents",
            spaces='drive',
            fields='nextPageToken, files(id, name, mimeType, size, parents)',
        ).execute().get('files', [])
        for item in items:
            self.update_permission(item['id'], recurse=True)
            if item['mimeType'] == FOLDER_MIME_TYPE:
                self._recursive_permission_helper(item['id'])
            if progress is not None:
                self.progress.advance(progress, advance=1)

    def update_permission(
        self,
        *items: Item | str,
        recurse: bool = False
    ) -> dict[str, list[str]]:
        permissions = dict()
        permission_task = None

        if not recurse:
            permission_task = self.progress.add_task(
                "[magenta]Granting permissions", total=len(items))

        for item in items:
            if not isinstance(item, str):
                item_id = item.id
            else:
                item_id = item
            permissions[item_id] = self._permission_helper(item_id)
            if not recurse and permission_task is not None:
                self.progress.advance(permission_task, advance=1)
        return permissions

    def _permission_helper(self, file_id: str):
        error_count = 0
        while error_count <= 5:
            try:
                permission = {'type': 'anyone',
                              'value': 'anyone',
                              # 'role': 'writer'}
                              'role': 'writer'}
                changed_permission = self.service.permissions().create(
                    fileId=file_id,
                    body=permission
                ).execute()
                self.progress.log("changed_permission = ", changed_permission)
                break
            except HttpError as error:
                self.progress.log(
                    '[bold red]ERROR[/bold red]',
                    "While granting permission",
                    error,
                    log_locals=True
                )
                if error.reason == 'Internal Error':
                    error_count += 1
                    self.progress.log(f"Retrying {error_count} ...")
                else:
                    break

    @folder_to_id
    def review_copy(
        self,
        source: ItemID,
        destination: ItemID
    ) -> CopyStats:
        # TODO: Use TypedDict / SimpleNamespaces / NamedTuple for result
        copied = []
        not_copied = []
        total_files = 0
        nmatches = 0

        files_from_parent = self._get_files_from_parent(source)
        self.progress.log(
            "All files and their parents found. Starting review.")

        review_task = self.progress.add_task(
            "[green]Reviewing", total=len(files_from_parent))
        for file, parent in files_from_parent:
            total_files += 1
            fname = file.name.replace("'", "\\'")
            query = f"""name='{fname}'"""  # and '{destination}' in parents"""
            search_results = self.search(query, driveId=destination)
            for match in search_results:
                if isinstance(match, Folder):
                    continue
                # parent_ids = match.parents
                # if len(parent_ids) > 1:
                #     breakpoint()
                # parents_in_dest = [
                #     self.search_by_id(parent_id) for parent_id in parent_ids
                # ]
                # matching_parents = any(
                #     p_dest.name == parent.name
                #     for p_dest in parents_in_dest
                # )
                if (file.size == match.size):   # and matching_parents:
                    nmatches += 1
                    copied.append(file)
                    break
            else:
                not_copied.append(file)
            self.progress.advance(review_task, advance=1)

        all_copied = True if nmatches == total_files else False
        if copied:
            with open("copied.log", 'w+') as fh:
                for item in copied:
                    fh.write(f'{item}\n')
        if not_copied:
            with open("not_copied.log", 'w+') as fh:
                for item in not_copied:
                    fh.write(f'{item}\n')
        self.progress.log(
            f"Review: {all_copied=}, {len(copied)=}, {len(not_copied)=}"
        )
        total_size = sum(item.size for item in copied)
        fmt = format_size(total_size)
        self.progress.log(
            "Total copied content: {size:.3f} {unit!r}".format(**fmt)
        )
        return CopyStats(all_copied, copied, not_copied, fmt)


def categorize(item: FileType | FolderType) -> Item:
    try:
        return File(**item) if 'size' in item else Folder(**item)
    except ValidationError as err:
        print(f"Not an item: {item}", err)
        raise ValueError from err


@cache
def total_size(item: Item) -> int:
    """
    Return size of a file or folder in bytes.
    """
    # TODO: Use size-related functions
    if isinstance(item, File):
        return item.size
    total = 0
    gdrive = DriveService()
    for item in gdrive.list_dir(item.id):
        total += total_size(item)
    return total


def per_item_size(
        item: Item,
        *,
        per_item: Literal[True]
) -> Generator[int, None, None]:
    if isinstance(item, File):
        yield item.size
    gdrive = DriveService()
    for item in gdrive.list_dir(item.id):
        yield from per_item_size(item, per_item=per_item)


@overload
def size_on_disk(
    item: Item,
    *,
    per_item: Optional[Literal[False]] = False
) -> int:
    ...


@overload
def size_on_disk(
    item: Item,
    *,
    per_item: Literal[True]
) -> Generator[int, None, None]:
    ...


def size_on_disk(
    item: Item,
    *,
    per_item: Optional[bool] = False
) -> int | Generator[int, None, None]:
    if per_item:
        return per_item_size(item, per_item=per_item)
    return total_size(item)
