from urllib.parse import quote as urllib_quote, unquote as urllib_unquote


BASE_DIR = "package-downloads"
DATE_FORMAT = "%Y-%m-%d"
CHANNELS = {
    "bioconda": "https://conda.anaconda.org/bioconda",
}
SUBDIRS = (
    "noarch",
    "linux-64",
    "linux-aarch64",
    "linux-ppc64le",
    "osx-64",
    "osx-arm64",
    "win-64",
)


def escape_path(path: str) -> str:
    return urllib_quote(path).replace("//", "%2F/").replace("%", "=")


def unescape_path(path: str) -> str:
    return urllib_unquote(path.replace("=", "%"))
