#!/usr/bin/env python3
"""Pack, unpack and check the seed corpus, one archive per target, the same on any OS"""

import gzip
import io
import os
import sys
import tarfile

HERE = os.path.dirname(os.path.abspath(__file__))
TARGETS = ("format_parsers", "codec_roundtrip", "persisted_index", "store_ops")
MTIME = 946684800


def corpus(target):
    return os.path.join(HERE, "corpus", target)


def archive(target):
    return os.path.join(HERE, "seeds", target + ".tar.gz")


def tarred(target):
    """The corpus as one ustar stream, fixed on everything but the unit bytes"""
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        for name in sorted(os.listdir(corpus(target))):
            path = os.path.join(corpus(target), name)
            if name.startswith(".") or not os.path.isfile(path):
                continue
            unit = tarfile.TarInfo(name)
            unit.size = os.path.getsize(path)
            unit.mtime = MTIME
            unit.mode = 0o644
            unit.uid = unit.gid = 0
            unit.uname = unit.gname = ""
            with open(path, "rb") as body:
                tar.addfile(unit, body)
    return out.getvalue()


def pack(target):
    out = io.BytesIO()
    with gzip.GzipFile(fileobj=out, mode="wb", compresslevel=9, mtime=0) as gz:
        gz.write(tarred(target))
    with open(archive(target), "wb") as file:
        file.write(out.getvalue())
    units = sum(1 for unit in os.scandir(corpus(target)) if not unit.name.startswith("."))
    print(f"{target}: {units} units, {len(out.getvalue())} bytes")


def unpack(target):
    os.makedirs(corpus(target), exist_ok=True)
    with tarfile.open(archive(target)) as tar:
        try:
            tar.extractall(corpus(target), filter="data")
        except TypeError:
            tar.extractall(corpus(target))
    print(f"{target}: unpacked into {os.path.relpath(corpus(target), HERE)}")


def check(target):
    """The committed archive holds what the corpus packs to, compared under the gzip"""
    with gzip.open(archive(target), "rb") as gz:
        committed = gz.read()
    if committed != tarred(target):
        sys.exit(f"{target}: seeds/{target}.tar.gz is not what corpus/{target} packs to")
    print(f"{target}: seeds match the corpus")


def main(argv):
    verbs = {"pack": pack, "unpack": unpack, "check": check}
    if len(argv) < 2 or argv[1] not in verbs:
        sys.exit(f"usage: seeds.py pack|unpack|check [{'|'.join(TARGETS)}]...")
    for target in argv[2:] or TARGETS:
        if target not in TARGETS:
            sys.exit(f"{target} is not a fuzz target")
        verbs[argv[1]](target)


if __name__ == "__main__":
    main(sys.argv)
