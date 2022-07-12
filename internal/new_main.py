from .datatypes import Unit
from .service import DriveService

FILMS = "1uo8fbXVIfx3DLQAP1Q60lpEumzGISpBw"
SHARED_FILMS = "1wZKWmoowsMCB8ffb5vB_dn-bHD2JNT4o"
# SHARED_FILMS_DUMMY = "1PExMnF_y1MA0nb3-UvCshYWtSlcXgKn7"

# TEST ONLY
# FILMS = "1PUA7WRzcbGb3heLN7VrEZKBV5DzBx4Hg"
# SHARED_FILMS_DUMMY = "1T_YMoXocmaRiFiUTOhxnvlQN6lTzCT8s"

with DriveService() as gdrive:
    items = gdrive.list_dir(FILMS, log=True)
    clusters = gdrive.make_cluster(items, upper_limit=3 * Unit.TB, exclude={'Move_Films_1'})
    cluster = next(clusters)

    new_folder = gdrive.create_folder("Move_Films_1", destination=FILMS)
    fid, name = new_folder.id, new_folder.name
    # fid, name = "1h4dD851WKoOHpt5tCIfuQQZ_VZxstq20", "Move_Films_1"
    gdrive.move(cluster, destination=fid)
    # gdrive.move(cluster, destination=new_folder.id)
    main_tree = gdrive.build_tree(fid)[fid]['items']
    gdrive.progress.log(f"Moving items of {name}")
    all_files_moved = gdrive.move_tree(main_tree, SHARED_FILMS, source=fid, name=name)
    # main_tree = gdrive.build_tree(new_folder.id)[new_folder.id]['items']
    # gdrive.progress.log(f"Moving items of {new_folder.name}")
    # all_files_moved = gdrive.move_tree(main_tree, SHARED_FILMS, source=new_folder.id, name=new_folder.name)
    # gdrive.progress.log(f"{all_files_moved=}")

    # fid, name = "10c2x70g-tZbDIgIKnaLvpT8NiQxy9zzc", ""
    # fid, name = FILMS, 'Test_Delete'


