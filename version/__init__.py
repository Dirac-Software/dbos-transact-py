import sys
from pdm.backend.hooks.version import SCMVersion

# DBOS uses UUIDv7, but only in Python 3.14 and later
assert sys.version_info >= (3, 14)


def format_version(git_version: SCMVersion) -> str:
    """
    Format version into string.

    1. Release versions may only be published from release branches. Their version is a git tag.
    2. Preview versions are published from main. They are PEP440 alpha releases whose version is the
    next release version number followed by "a" followed by the number of commits since the last release.
    If the last release was 1.2.3 and there have been ten commits since, the preview version is 1.2.3a10
    3. Test versions are published from feature branches. They are PEP440 local versions tagged with a git hash.
    """
    assert git_version.branch is not None
    is_release = "release" in git_version.branch
    is_preview = git_version.branch == "main"

    next_version = guess_next_version(str(git_version.version))

    if git_version.distance is None:
        if is_release:
            version = str(git_version.version)
        elif is_preview:
            version = f"{next_version}a0"
        else:
            version = f"{next_version}a0+{git_version.node}"
    else:
        if is_release:
            raise Exception(
                f"Release branches may only publish tagged releases. Distance: {git_version.distance}"
            )
        elif is_preview:
            version = f"{next_version}a{git_version.distance}"
        else:
            version = f"{next_version}a{git_version.distance}+{git_version.node}"

    return version


def guess_next_version(version_number: str) -> str:
    parts = list(map(int, version_number.split(".")))
    while len(parts) < 3:
        parts.append(0)
    major, minor, patch = parts[0], parts[1], parts[2]
    minor += 1
    return f"{major}.{minor}.{patch}"
