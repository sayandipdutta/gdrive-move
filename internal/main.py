import sys
from .datatypes import Item, Unit, Cluster
from .service import DriveService, size_on_disk


TEST = CLUSTER = NEW_FOLDER = MOVE = COPY = DELETE = REVIEW = PERMISSION = 0

if __name__ == '__main__':
    # main()
    # breakpoint()
    # MAX_CLUSTER_SIZE = 700 * Unit.GB
    MAX_CLUSTER_SIZE = 800 * Unit.GB

    SOURCE, HR_NAME, cluster_prepend, dp = "1JM4RkZxbV65gDGFVWvqRiCL_lpD-EuVA", "KG Freeleech", 'KG', 'Films'
    SOURCE, HR_NAME, cluster_prepend, dp = "1XvhVCE1s1uRZgx3fFTnKITPTXszVZ1eC", "Series", 'Series', 'BGFA_Series'
    SOURCE, HR_NAME, cluster_prepend, dp = "1uo8fbXVIfx3DLQAP1Q60lpEumzGISpBw", "Films", 'Films', 'BGFA_Films'
    DEST = "0AEaJmSa7kQbMUk9PVA"
    copy_log = 'copy.log'

    # TEST = True
    # CLUSTER = True
    # NEW_FOLDER = True
    # MOVE = True
    # PERMISSION = True
    COPY = True
    REVIEW = True
    # DELETE = True

    with DriveService() as gdrive:
        # initial values
        # WARNING: Make sure new_folder is updated (if NEW_FOLDER==False)
        new_folder = "1-P9E5b8I-w4eMP-3nFuLHTye6hp9HcQu"
        link = f"https://drive.google.com/drive/u/2/folders/{new_folder}"
        cluster_name = f'{cluster_prepend}_4'
        cluster: Cluster[Item] = Cluster()
        all_copied = False
        size_hint = None
        tot = None

        if TEST:
            resp = gdrive.service.files().list(
                q="name = 'Adoption (1975).srt'",
                fields="nextPageToken, files(id,name, parents, mimeType, size)"
            ).execute().get('files', [])

        if not NEW_FOLDER:
            gdrive.progress.log("NEW_FOLDER is False")
            gdrive.progress.log(f"Current value of {new_folder=}")
            gdrive.progress.log(f"[green]Cluster name:[yellow]{cluster_name}")
            gdrive.progress.log(f"[green]Target name:[yellow]{dp}")
            gdrive.progress.log(f"Link to new folder: {link}")
            response = input(
                "[bold red]Correct value set[/bold red] 'new_folder'? "
                "[yellow]verify in above link[/yellow]. "
                "[green]y[/green]/[red]n[/red]:"
            )
            if response.lower() != 'y':
                sys.exit(1)

        if CLUSTER:
            items = gdrive.list_dir(SOURCE, log=True)
            exclude = {"Series_1", "Films_1", "Films_2", "Films_3"}
            clusters = gdrive.make_cluster(
                items,
                upper_limit=MAX_CLUSTER_SIZE,
                exclude=exclude
            )
            cluster = next(clusters)
            with open(copy_log, 'w+') as fh:
                for item in cluster:
                    fh.write(f'{HR_NAME}::{cluster_prepend}::{item.name}\n')
            gdrive.progress.log(
                f"Current cluster: {cluster}"
            )
            tot = cluster.nitems
            size_hint = cluster.size

        if NEW_FOLDER:
            new_folder = gdrive.create_folder(
                cluster_name,
                destination=SOURCE
            ).id

        if MOVE:
            gdrive.move(cluster, destination=new_folder)

        if PERMISSION:
            gdrive.update_permission_recursively(new_folder, total=tot)

        if COPY:
            # if not CLUSTER:
            #     new_folder_ = gdrive.search_by_id(new_folder)
            #     size_hint = sum(size_on_disk(new_folder_, per_item=True))
            #     new_folder = new_folder_.id
            # size_hint = int(25.7 * Unit.GB)
            gdrive.copy(source=new_folder, destination=DEST, dest_path=dp,
                        port="5572", size_hint=size_hint, timeout=900)

        if REVIEW:
            stats = all_copied, *_ = gdrive.review_copy(
                source=new_folder,
                destination=DEST
            )

        if DELETE:
            if all_copied:
                gdrive.delete(new_folder)
