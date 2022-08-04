from .datatypes import Unit
from .service import DriveService

SOURCE = "1XvhVCE1s1uRZgx3fFTnKITPTXszVZ1eC"
DESTINATION = "17V7dcxuSPiMWqOYhDKc-_PBi-Hhq-Ogd"

with DriveService() as gdrive:
    items = gdrive.list_dir(SOURCE, log=True)
    clusters = gdrive.make_cluster(items, upper_limit=3 * Unit.TB, exclude={'Move_Series_1'}, max_clusters=1)
    new_folder = gdrive.create_folder("Move_Series_1", destination=SOURCE)
    fid, name = new_folder.id, new_folder.name
    for cluster in clusters:
        cluster = next(clusters)
        gdrive.move(cluster, destination=fid)

    main_tree = gdrive.build_tree(fid)[fid]['items']
    gdrive.progress.log(f"Moving items of {name}")
    all_files_moved = gdrive.move_tree(main_tree, DESTINATION, source=fid, name=name)

    if all_files_moved:
        gdrive.delete(fid)

