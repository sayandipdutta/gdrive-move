from dataclasses import dataclass, field
from enum import IntEnum, auto
from functools import wraps
from inspect import signature
from typing import (
    Callable,
    ClassVar,
    Generic,
    Literal,
    NamedTuple,
    ParamSpec,
    TypeAlias,
    TypeVar,
    TypedDict,
    Union,
)

from pydantic import (
    BaseModel,
    validator,
    Extra,
    NonNegativeFloat,
    NonNegativeInt
)
from rich.console import Console
from rich.progress import (
    Progress,
    TransferSpeedColumn,
    SpinnerColumn,
    TimeElapsedColumn,
    Task,
    Text
)


__all__ = (
    "FOLDER_MIME_TYPE",
    "ItemID",
    "Item",
    "folder_to_id",
    "File",
    "Folder",
    "FileType",
    "FolderType",
    "Unit",
    "Size",
    "SupportRich",
)

FOLDER_MIME_TYPE: Literal['application/vnd.google-apps.folder'] = \
    'application/vnd.google-apps.folder'

ItemID: TypeAlias = str
Item: TypeAlias = Union['File', 'Folder']

P = ParamSpec("P")
T = TypeVar("T")
T_Item = TypeVar("T_Item", bound="Item")


class ConditionalTransferSpeedColumn(TransferSpeedColumn):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def render(self, task: Task) -> Text:
        if task.fields.get('show_speed'):
            return super().render(task)
        else:
            return Text("")


columns = [
    SpinnerColumn(),
    *Progress.get_default_columns(),
    TimeElapsedColumn(),
    ConditionalTransferSpeedColumn(),
]


@dataclass(kw_only=True)
class SupportRich:
    console: Console = Console()

    def __post_init__(self):
        self.progress = Progress(
            *columns,
            console=self.console,
            transient=False,
        )


class AutoSize(IntEnum):
    def _generate_next_value_(name, start, count, last_value):
        return 1024 ** count

    def __repr__(self):
        return f"<{self.__class__.__name__}.{self.name}>"


class Response(TypedDict):
    id: str
    name: str
    mimeType: str
    parents: list[str]


class FolderType(Response):
    pass


class FileType(Response):
    size: str
    md5Checksum: str


class Unit(AutoSize):
    B = auto()
    KB = auto()
    MB = auto()
    GB = auto()
    TB = auto()


class Size(TypedDict):
    size: NonNegativeFloat
    unit: Unit


def format_size(size_in_bytes: int) -> Size:
    """
    Format size in bytes to human readable units.
    """
    size = float(size_in_bytes)
    for unit in (Unit.B, Unit.KB, Unit.MB, Unit.GB):
        if size < 1024:
            return {"size": size, "unit": unit}
        size /= 1024
    return {"size": size, "unit": Unit.TB}


class File(BaseModel, extra=Extra.ignore):
    id: str
    name: str
    mimeType: str
    size: int
    parents: list[str]
    md5Checksum: str

    def __hash__(self):
        return hash(self.id)


class Folder(BaseModel, extra=Extra.ignore):
    id: str
    name: str
    mimeType: str
    parents: list[str]

    @validator('mimeType')
    def fixed_mimeType(cls, v):
        if v != FOLDER_MIME_TYPE:
            raise ValueError("Not a valid Google Drive Folder.")
        return v

    def __hash__(self):
        return hash(self.id)


class CopyStats(NamedTuple):
    all_copied: bool
    copied: list[File]
    not_copied: list[File]
    size: Size


@dataclass
class Cluster(Generic[T_Item]):
    items: ClassVar[dict[int, 'Cluster']]

    cluster: list[T_Item] = field(default_factory=list[T_Item])
    size: NonNegativeInt = 0
    nitems: int = 0

    def __post_init__(self):
        self._hr_size: Size = format_size(self.size)

    def __iter__(self):
        return iter(self.cluster)

    def __next__(self):
        for item in self.cluster:
            yield item

    def __str__(self):
        return (
            f"{self.__class__.__name__}"
            f"(cluster=[...], size={self._hr_size}, nitems={self.nitems})"
        )


class FileTree(dict):
    def __missing__(self, key: str) -> 'FileTree':
        value = self[key] = type(self)()
        return value


def folder_to_id(func: Callable[P, T]) -> Callable[P, T]:
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        sig = signature(func)
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        for name, param in sig.parameters.items():
            if param.annotation == ItemID:
                if isinstance(bound.arguments[name], Folder):
                    bound.arguments[name] = bound.arguments[name].id
        return func(*bound.args, **bound.kwargs)
    return wrapper
