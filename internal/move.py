from pydantic import ValidationError

from .datatypes import FileTree, ItemID
from .service import DriveService

SOURCE = "14qN0kyHzHEfmRXWB9l-vb5sl3DXy9L_G"
DESTIN = "1PExMnF_y1MA0nb3-UvCshYWtSlcXgKn7"


def recreate_folder_structure(
    tree: FileTree,
    at: ItemID,
    gdrive: DriveService,
    indent: str = '',
    name: str | None = None
):
    print(f"{indent}{name}")
    indent += '\t'
    for item, value in tree.items():
        if value['kind'] == "File":
            print(f"{indent}{value['info'].name}")
            if not value['info'].trashed == 'true':
                gdrive.move(value['info'], destination=at, supportsAllDrives=True)
        else:
            folder = gdrive.create_folder(value['info'].name, destination=at, supportsAllDrives=True)
            recreate_folder_structure(value['items'], folder.id, gdrive, indent, value['info'].name)


with DriveService() as gdrive:
    main_tree = gdrive.build_tree(SOURCE)[SOURCE]['items']
    recreate_folder_structure(main_tree, DESTIN, gdrive, '', 'Dummy_Films')
