from .datatypes import Unit
from .service import DriveService

FILMS = "1uo8fbXVIfx3DLQAP1Q60lpEumzGISpBw"
SHARED_FILMS = "1KrlKzzGHhnt0-Zan-t46Oc4A7cZNZ4uM"
SHARED_FILMS_DUMMY = "1PExMnF_y1MA0nb3-UvCshYWtSlcXgKn7"

with DriveService() as gdrive:
    items = gdrive.list_dir(FILMS, log=True)
    exclude = {"Series_1", "Films_1", "Films_2", "Films_3", "Films_4", "Films_5"}
    clusters = gdrive.make_cluster(items, upper_limit=1 * Unit.TB, exclude=exclude)
    cluster = next(clusters)
    gdrive.progress.log(f"Current cluster: {cluster}")

    new_folder = gdrive.create_folder("Move_Films_1", destination=FILMS)
    gdrive.move(cluster, destination=new_folder.id)
    main_tree = gdrive.build_tree(new_folder.id)[new_folder.id]['items']
    gdrive.move_tree(main_tree, SHARED_FILMS_DUMMY, source=new_folder.id, name=new_folder.name)

    fid = "10c2x70g-tZbDIgIKnaLvpT8NiQxy9zzc"
    fids = {
            'Films_2': '13bnT5hx6E4QscBhoBHUFUw9UmAKQItSf',
            'Films_3': '1DsttSwvao9KkQSsmOSp5Y1u_m-2Ze4wZ',
            'Films_4': '1-P9E5b8I-w4eMP-3nFuLHTye6hp9HcQu',
            'Films_5': '12RAZlGpwveRgqyd0d_j3pgIrWZfawXQi',
            }
    for name, fid in fids.items():
        gdrive.progress.log(f"Moving items of {name}")
        main_tree = gdrive.build_tree(fid)[fid]['items']
        gdrive.move_tree(main_tree, SHARED_FILMS_DUMMY, source=fid, name=name)


