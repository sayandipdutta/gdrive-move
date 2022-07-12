# HACK: Try to copy in separate process, and communicate to update progress. ✅
# PERF: Check which functions take the longest.
# PERF: Try async or threading/multiprocessing.
# OPTIMIZE: Build a complete tree, and reuse when needed. ✅
# OPTIMIZE: Use cache.
# FIX: Write tests.
# TODO: Add custom errors.
# TODO: Put all the settings in config file.
# TODO: Init git. ✅
# TODO: Add logging.
# TODO: Add docstring.


from functools import cache
from io import TextIOWrapper
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

from . import TOKEN, CREDS, config
from .datatypes import (
    CopyStats,
    Cluster,
    File,
    Folder,
    FileTree,
    Response,
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
    def creds(self) -> Credentials:
        return self._creds

    @property
    def service(self) -> Resource:
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

        

    def build_tree(self, source: ItemID, *, from_: Iterable[Item] | None = None) -> FileTree:
        file_tree = FileTree()
        item = self.search_by_id(source)
        assert isinstance(item, Folder), "Can only build FileTree from Folder."
        self.progress.log("Building tree...")

        def _recursor(
            source: Folder,
            file_tree: FileTree,
            ancestors: list[Item] = list(),
            from_: Iterable[Item] | None = None
        ) -> FileTree:
            item_id = source.id
            folder = file_tree[item_id]
            folder['kind'] = 'Folder'
            folder['info'] = source
            folder['ancestors'] = (ancestors[:])
            folder['nitems'] = 0
            folder['size'] = 0
            content = self.list_dir(item_id) if from_ is None else from_
            new_ancestors = [*ancestors, folder]
            for it in content:
                if isinstance(it, File):
                    item_table = folder['items'][it.id]
                    item_table['kind'] = 'File'
                    item_table['info'] = it
                    item_table['ancestors'] = (new_ancestors)
                    item_table['size'] = it.size
                    folder['size'] += it.size
                else:
                    folder['items'] = _recursor(
                        it, folder['items'], new_ancestors)
                    folder['size'] += folder['items'][it.id]['size']
                folder['nitems'] += 1
            return file_tree
        file_tree = _recursor(item, file_tree, from_=from_)
        self.progress.log("Tree built.")
        return file_tree

    def is_contained(self, item: File, destination: ItemID) -> bool:
        escaped_name = item.name.replace("'", "\\'")
        query = f"name = '{escaped_name}'"
        results = self.search(query, driveId=destination, files_only=True)
        return any(item.md5Checksum == res.md5Checksum for res in results)

    def prune_copied(self, tree: FileTree, node: ItemID, destination: ItemID):
        root = tree[node]
        if root['kind'] == 'File':
            if self.is_contained(root['info'], destination):
                self.delete(node)
                for ancestor in root['ancestors']:
                    ancestor['size'] -= root['size']
                root['ancestors'][-1]['nitems'] -= 1
                del tree[node]
        else:
            for item in list(root['items']):
                self.prune_copied(root['items'], item, destination)
            if root['items'].is_empty():
                self.delete(node)
                if root['ancestors']:
                    root['ancestors'][-1]['nitems'] -= 1

    def move_tree(
        self,
        tree: FileTree,
        at: ItemID,
        source: ItemID = '',
        name: str = '',
        log: bool = True,
    ) -> bool:
        total_size = sum(value['size'] for value in tree.values())
        self.progress.log('size =' , format_size(total_size))
        task_id = self.progress.add_task("Moving tree...", total=total_size, show_speed=True)
        all_files_moved = False
        if log:
            with open(f'move_tree_log_{source}.log', 'w+') as logf:
                all_files_moved = self._move_tree_helper(tree, at, task_id, logfile=logf, name=name)
        else:
            all_files_moved = self._move_tree_helper(tree, at, task_id, name=name)
        self.progress.update(task_id, completed=total_size)
        self.progress.log(f"{all_files_moved=}")
        return all_files_moved


    def _move_tree_helper(
        self,
        tree: FileTree,
        at: ItemID,
        task_id: TaskID,
        indent: str = '',
        name: str = '',
        logfile: TextIOWrapper | None = None
    ) -> bool:
        text = f'{indent}{name}\n'
        print(text, file=logfile)
        indent += '\t'
        all_files_moved = True
        for item, value in list(tree.items()):
            val_name = value["info"].name
            text = f'{indent}{val_name}\n'
            if value['kind'] == "File":
                if not value['info'].trashed:
                    try:
                        self.move(value['info'], destination=at, supportsAllDrives=True)
                        print(text, file=logfile)
                        self.progress.advance(task_id, advance=value['size'])
                        for ancestor in value['ancestors']:
                            ancestor['size'] -= value['size']
                        ancestor['nitems'] -= 1
                        del tree[item]
                    except (TimeoutError, HttpError):
                        all_files_moved = False

            else:
                if not value['info'].trashed:
                    folder = self.create_folder(
                        value['info'].name,
                        destination=at,
                        supportsAllDrives=True
                    )
                    all_files_moved = self._move_tree_helper(
                        value['items'],
                        folder.id,
                        task_id=task_id,
                        name=val_name,
                        indent=indent,
                        logfile=logfile
                    )
                    if all_files_moved and value['size'] == 0 and value['nitems'] == 0:
                        self.delete(item)
                        value['ancestors'][-1]['nitems'] -= 1
                        del tree[item]
                    else:
                        all_files_moved = False
        return all_files_moved

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
                "fields": "files(id, name, mimeType, trashed, md5Checksum, size, parents)",
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
                        "files(id, name, mimeType, trashed, md5Checksum, size, parents)"
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
                cluster_ = Cluster(cluster[:], size, item_count)
                self.progress.log(f"Current cluster: {cluster_}")
                yield cluster_
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
        cluster_ = Cluster(cluster[:], size, item_count)
        self.progress.log(f"Current cluster: {cluster_}")
        yield cluster_
        self.progress.update(clustering_task, completed=size, total=size)

    @overload
    def move(
        self,
        item: Item,
        *,
        destination: ItemID,
        **kwargs: bool
    ) -> Item:
        ...

    @overload
    def move(
        self,
        item: Cluster[Item],
        *,
        destination: ItemID,
        **kwargs: bool
    ) -> list[Item]:
        ...

    @folder_to_id
    def move(
        self,
        item: Item | Cluster[Item],
        *,
        destination: ItemID,
        **kwargs: bool
    ) -> Item | list[Item]:
        if not isinstance(item, File | Folder):
            total = len(list(item))
            moving_task = self.progress.add_task(
                "[magenta]Moving files",
                total=total
            )

            items = []
            for each in item:
                try:
                    resp = self.move(each, destination=destination)
                    items.append(resp)
                    self.progress.advance(moving_task, advance=1)
                except (HttpError, TimeoutError) as error:
                    self.progress.log("ERROR: While moving", error)
            self.progress.log(f"Total top-level folders moved: {total}")

            return items

        else:
            self.progress.log(f"Moving {item.name} to {destination}")
            previous_parents = ", ".join(item.parents)
            try:
                resp = self.service.files().update(
                    fileId=item.id,
                    addParents=destination,
                    removeParents=previous_parents,
                    fields='id, name, mimeType, trashed, md5Checksum, size, parents',
                    **kwargs,
                ).execute()
                return categorize(resp)
            except HttpError as err:
                self.progress.log("ERROR: occurred while moving.", err,
                                  log_locals=True)
                raise
            except TimeoutError as err:
                self.progress.log("ERROR: occurred while moving.", err,
                                  log_locals=True)
                raise

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
        # FIX: Handle FileNotFoundError or KeyError
        cwd = config.getpath('PATHS', 'rclone_path',
                             fallback=Path()).expanduser()
        log_path = config.getpath(
            'PATHS', 'log_path', fallback=Path()).expanduser()
        rc_cmd = shlex.split(
            f'rclone rc --rc-addr="localhost:{port}" core/stats')
        start = time.perf_counter()
        size_bytes_done = 0
        printed_once = False
        prev_done = 0
        no_download = 0
        rclonelog = log_path / 'autorclone.log'
        with open(rclonelog, 'w+', encoding='utf-8', buffering=1) as fh:
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
                            self.progress.update(
                                copy_task, total=1, completed=1)
                            proc.kill()
                            self.progress.log(
                                f"[red]Timed Out[/red]: {timeout=}")
                            break
                        continue
                    except FileNotFoundError:
                        if time.perf_counter() - start > timeout:
                            self.progress.update(
                                copy_task, total=1, completed=1)
                            proc.kill()
                            self.progress.log(
                                f"[red]Timed Out[/red]: {timeout=}")
                            break
                        continue
                    response_processed = result.stdout.replace('\0', '')
                    response_processed_json = json.loads(response_processed)
                    size_bytes_done = int(response_processed_json['bytes'])
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
                    if not printed_once:
                        print(size_bytes_done)
                        printed_once = True
                    self.progress.update(
                        copy_task,
                        completed=size_bytes_done,
                    )
                    prev_done = size_bytes_done
        self.progress.log(
            "[bold green]COPY:[/bold green] copied -> "
            f"{format_size(size_bytes_done)}"
        )
        self.progress.update(copy_task, total=size_bytes_done,
                             completed=size_bytes_done)

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
        destination: ItemID,
        **kwargs: bool,
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
                fields="id, name, mimeType, trashed, md5Checksum, size, parents",
                **kwargs,
            ).execute()
        except HttpError as err:
            self.progress.log("[red]ERROR: While creating folder.", err)
            raise

        folder = Folder(**item)
        self.progress.log("New folder created", folder)
        return folder

    @overload
    def search(
        self,
        query: str,
        /,
        files_only: Literal[True],
        **kwargs: ItemID
    ) -> Generator[File, None, None]:
        ...

    @overload
    def search(
        self,
        query: str,
        /,
        files_only: Literal[False] = False,
        **kwargs: ItemID
    ) -> Generator[Item, None, None]:
        ...

    def search(
        self,
        query: str,
        /,
        files_only: bool = False,
        **kwargs: ItemID
    ) -> Generator[Item, None, None] | Generator[File, None, None]:
        query = query
        if files_only:
            query += f" mimeType != '{FOLDER_MIME_TYPE}'"
        try:
            page_token = None
            while True:
                response = self.service.files().list(   # type: ignore
                    q=query,
                    spaces='drive',
                    corpora='drive',
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                    fields=(
                        'nextPageToken, '
                        'files(id, name, mimeType, trashed, trashed, md5Checksum, size, parents)'
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

    @overload
    def search_by_id(
        self,
        id: str,
        *extra_fields: str,
        response: Literal[True]
    ) -> Response:
        ...

    @overload
    def search_by_id(
        self,
        id: str,
        *extra_fields: str,
        response: Literal[False] = False
    ) -> Item:
        ...

    def search_by_id(
        self,
        id: str,
        *extra_fields: str,
        response: bool = False
    ) -> Item | Response:
        # TODO: merge with search
        _fields = f", {', '.join(extra_fields)}" if extra_fields else ''
        item = self.service.files().get(
            fileId=id,
            supportsAllDrives=True,
            supportsTeamDrives=True,
            fields="id, name, mimeType, trashed, md5Checksum, size, parents" + _fields
        ).execute()
        if response:
            return item
        return categorize(item)

    def _get_files_from_parent(
        self,
        source: ItemID,
    ) -> list[File]:
        # TODO: merge with list_dir
        items = []
        files, total = self.list_dir(
            source, files_only=True, return_count=True
        )

        listing_task = self.progress.add_task(
            "[yellow]Gathering parents",
            total=total
        )
        for item in files:
            self.progress.advance(listing_task, advance=1)
            items.append(item)
        return items

    @folder_to_id
    def update_permission_recursively(self, folder_id: ItemID, total: int = None):
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
            fields='nextPageToken, files(id, name, mimeType, trashed, md5Checksum, size, parents)',
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
        for file in files_from_parent:
            total_files += 1
            fname = file.name.replace("'", "\\'")
            query = f"name='{fname}'"
            search_results = self.search(query, driveId=destination)
            for match in search_results:
                if isinstance(match, Folder):
                    continue
                if (file.md5Checksum == match.md5Checksum):
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

