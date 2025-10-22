#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Semantic (file-aware) CDC for OCI layer blobs.
- Detects gzip/zstd and decompresses transparently
- Untars layers and chunks per-file (whole-file CAS for small, FastCDC for large)
- Deterministic repack (no compression) for verification/transport

Requires:
  pip install fastcdc zstandard
"""

from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional
import argparse, hashlib, json, tarfile, tempfile, time

# Optional dependency for zstd streams
try:
    import zstandard as zstd  # optional
except Exception:
    zstd = None

from fastcdc import fastcdc  # pip install fastcdc

MAGIC_GZIP = b"\x1f\x8b"
MAGIC_ZSTD = b"\x28\xb5\x2f\xfd"

def sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256(); h.update(data); return h.hexdigest()

def sha256_file(p: Path, bufsize=1024*1024) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        while True:
            b = f.read(bufsize)
            if not b: break
            h.update(b)
    return h.hexdigest()

def is_gzip(p: Path) -> bool:
    with p.open("rb") as f: return f.read(2) == MAGIC_GZIP

def is_zstd(p: Path) -> bool:
    with p.open("rb") as f: return f.read(4) == MAGIC_ZSTD

def stream_decompress_to_file(src: Path, dst: Path) -> None:
    """Decompress gzip or zstd (if available) to dst, else copy as-is."""
    if is_gzip(src):
        import gzip
        with gzip.open(src, "rb") as rf, dst.open("wb") as wf:
            while True:
                b = rf.read(1024*1024)
                if not b: break
                wf.write(b)
        return
    if is_zstd(src) and zstd is not None:
        dctx = zstd.ZstdDecompressor()
        with src.open("rb") as rf, dst.open("wb") as wf: dctx.copy_stream(rf, wf)
        return
    # Not compressed (or unknown), copy
    dst.write_bytes(src.read_bytes())

def normalize_tarinfo(info: tarfile.TarInfo) -> tarfile.TarInfo:
    info.uid=0; info.gid=0; info.uname=""; info.gname=""; info.mtime=0
    info.mode = info.mode & 0o777
    return info

@dataclass
class ChunkEntry:
    file: str
    length: int
    sha256: str

@dataclass
class FileEntry:
    path: str
    mode: int
    size: int
    type: str
    linkname: Optional[str]
    chunks: List[ChunkEntry]
    sha256: Optional[str]
    duplication: int

@dataclass
class LayerRecord:
    blob_name: str
    blob_sha256: str
    method: str
    params: Dict[str, int]
    files: List[FileEntry]

def file_type_from_tarinfo(t: tarfile.TarInfo) -> str:
    if t.isreg(): return "reg"
    if t.isdir(): return "dir"
    if t.islnk(): return "lnk"
    if t.issym(): return "sym"
    if t.ischr() or t.isblk(): return "dev"
    if t.isfifo(): return "fifo"
    return "other"

def detect_tarfile(p: Path) -> bool:
    try:
        if p.stat().st_size == 0:
            return False
        return tarfile.is_tarfile(str(p))
    except Exception:
        return False

def oci_layer_digests_from_index(oci_root: Path) -> set:
    """Return set of sha256 hex digests that are layer blobs by reading index.json -> manifest(s)."""
    try:
        idx = json.loads((oci_root / "index.json").read_text(encoding="utf-8"))
        digests = set()
        manifests = idx.get("manifests") or []
        for m in manifests:
            d = m.get("digest","")
            if not d.startswith("sha256:"):
                continue
            # read manifest blob
            mblob = oci_root / "blobs" / "sha256" / d.split(":",1)[1]
            man = json.loads(mblob.read_text(encoding="utf-8"))
            for layer in man.get("layers", []):
                mt = layer.get("mediaType","")
                if "layer" in mt:  # include both tar and tar+compression
                    ld = layer.get("digest","")
                    if ld.startswith("sha256:"):
                        digests.add(ld.split(":",1)[1])
        return digests
    except Exception:
        return set()

def chunk_file_cdc(fpath: Path, chunks_root: Path, min_sz: int, avg_sz: int, max_sz: int, small_cutover: int):
    """Return (chunks, file_sha, dup_skipped_count, is_wholefile)."""
    size = fpath.stat().st_size
    if size <= small_cutover:
        data = fpath.read_bytes()
        sha = sha256_bytes(data)
        out = chunks_root / f"{sha}.bin"
        dup_skipped = 0
        if not out.exists():
            out.write_bytes(data)
        else:
            dup_skipped = 1
        return [ChunkEntry(file=f"{sha}.bin", length=len(data), sha256=sha)], sha, dup_skipped, True

    entries: List[ChunkEntry] = []
    file_sha = hashlib.sha256()
    dup_skipped = 0
    with fpath.open("rb") as rf:
        segments = list(fastcdc(str(fpath), min_sz, avg_sz, max_sz))
        for seg in segments:
            rf.seek(seg.offset); data = rf.read(seg.length)
            csha = sha256_bytes(data); cname = f"{csha}.bin"
            out = chunks_root / cname
            if not out.exists():
                out.write_bytes(data)
            else:
                dup_skipped += 1
            entries.append(ChunkEntry(file=cname, length=seg.length, sha256=csha))
            file_sha.update(data)
    return entries, file_sha.hexdigest(), dup_skipped, False

def process_layer_semantic(
    blob_path: Path,
    work_root: Path,
    chunks_root: Path,
    min_sz: int,
    avg_sz: int,
    max_sz: int,
    small_cutover: int,
    pre_decompressed_tar: Optional[Path] = None,
) -> LayerRecord:
    """Process one blob: use pre_decompressed_tar if provided to avoid double decompression."""
    if pre_decompressed_tar is not None:
        layer_tmp = pre_decompressed_tar
    else:
        layer_tmp = work_root / (blob_path.name + ".tar")
        layer_tmp.parent.mkdir(parents=True, exist_ok=True)
        stream_decompress_to_file(blob_path, layer_tmp)

    files: List[FileEntry] = []
    if not detect_tarfile(layer_tmp):
        raise tarfile.ReadError(f"decompressed blob is not a tar: {blob_path.name}")

    with tarfile.open(layer_tmp, "r") as tf:
        for member in tf.getmembers():
            rel = member.name.lstrip("./")
            ftype = file_type_from_tarinfo(member)
            linkname = getattr(member, "linkname", None)

            if ftype != "reg":
                files.append(FileEntry(path=rel, mode=member.mode & 0o777, size=member.size, type=ftype,
                                       linkname=linkname, chunks=[], sha256=None, duplication=0))
                continue

            extracted = tf.extractfile(member)
            if extracted is None:
                files.append(FileEntry(path=rel, mode=member.mode & 0o777, size=member.size, type=ftype,
                                       linkname=linkname, chunks=[], sha256=None, duplication=0))
                continue

            with tempfile.NamedTemporaryFile(delete=False) as tmpf:
                tmp_path = Path(tmpf.name)
                while True:
                    b = extracted.read(1024*1024)
                    if not b: break
                    tmpf.write(b)

            chunks, fsha, dup_skipped, is_whole = chunk_file_cdc(tmp_path, chunks_root, min_sz, avg_sz, max_sz, small_cutover)
            tmp_path.unlink(missing_ok=True)
            files.append(FileEntry(path=rel, mode=member.mode & 0o777, size=member.size, type=ftype,
                                   linkname=linkname, chunks=chunks, sha256=fsha, duplication=dup_skipped))

    return LayerRecord(
        blob_name=blob_path.name,
        blob_sha256=sha256_file(blob_path),
        method="file-semantic",
        params={"min": min_sz, "avg": avg_sz, "max": max_sz, "small_cutover": small_cutover},
        files=files
    )

def pack_semantic_to_tar(rec: LayerRecord, chunks_root: Path, out_tar: Path) -> None:
    out_tar.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(out_tar, "w") as tf:
        for f in sorted(rec.files, key=lambda x: (x.type != "dir", x.path)):
            ti = tarfile.TarInfo(f.path); ti = normalize_tarinfo(ti)
            if f.type == "dir":
                ti.type = tarfile.DIRTYPE; ti.mode = f.mode or 0o755; tf.addfile(ti); continue
            if f.type == "sym":
                ti.type = tarfile.SYMTYPE; ti.linkname = f.linkname or ""; tf.addfile(ti); continue
            if f.type in ("lnk","dev","fifo","other"):
                tf.addfile(ti); continue
            data_paths = [chunks_root / c.file for c in f.chunks]
            ti.size = sum(p.stat().st_size for p in data_paths)
            ti.type = tarfile.REGTYPE; ti.mode = f.mode or 0o644
            tf.addfile(ti)
            for p in data_paths:
                with p.open("rb") as rf:
                    while True:
                        b = rf.read(1024*1024)
                        if not b: break
                        tf.fileobj.write(b)

# -------- Extra commands: repack-all & verify --------

def cmd_repack_all(meta_path: Path, out_root: Path) -> None:
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    chunks_root = Path(meta["chunks_root"])
    records = meta["records"]
    out_blobs = out_root / "blobs" / "sha256"
    out_blobs.mkdir(parents=True, exist_ok=True)

    total_bytes = 0
    total_layers = 0
    t0 = time.perf_counter()
    for r in records:
        lr = LayerRecord(
            blob_name=r["blob_name"], blob_sha256=r["blob_sha256"],
            method=r["method"], params=r["params"],
            files=[FileEntry(**f) for f in r["files"]]
        )
        dest = out_blobs / (lr.blob_name + ".tar")
        pack_semantic_to_tar(lr, chunks_root, dest)
        sz = dest.stat().st_size
        total_bytes += sz
        total_layers += 1
        print(f"  - repacked {lr.blob_name}.tar size={sz}")

    elapsed = time.perf_counter() - t0
    thr = (total_bytes/1_048_576)/elapsed if elapsed>0 else 0
    print("--- 성능 (repack-all) ---")
    print(f"  레이어 수: {total_layers}")
    print(f"  총 크기: {total_bytes} B")
    print(f"  총 시간: {elapsed:.3f} s  처리량: {thr:.2f} MB/s")

def cmd_verify_semantic(oci_root: Path, repacked_root: Path) -> None:
    """Compare sha256 of decompressed original tar vs repacked tar per layer."""
    blobs_dir = oci_root / "blobs" / "sha256"
    rep_dir = repacked_root / "blobs" / "sha256"
    ok = bad = 0
    for p in sorted(blobs_dir.iterdir()):
        if not p.is_file():
            continue
        # decompress original to temp tar
        tmp = repacked_root / (p.name + ".orig.tar")
        stream_decompress_to_file(p, tmp)
        if not tarfile.is_tarfile(str(tmp)):
            tmp.unlink(missing_ok=True)
            print(f"[-] skip non-tar blob {p.name}")
            continue
        # repacked path
        rp = rep_dir / (p.name + ".tar")
        if not rp.exists():
            print(f"[!] missing repacked for {p.name}")
            tmp.unlink(missing_ok=True)
            bad += 1
            continue
        s1 = sha256_file(tmp); s2 = sha256_file(rp)
        tmp.unlink(missing_ok=True)
        if s1 == s2:
            ok += 1
        else:
            print(f"[X] 해시 불일치: {p.name} {s1[:16]}.. != {s2[:16]}..")
            bad += 1
    print(f"[=] verify-semantic: ok={ok}, bad={bad}, total={ok+bad}")
    if bad > 0:
        raise SystemExit(1)

# ---------------------------- CLI ----------------------------

def main():
    ap = argparse.ArgumentParser(description="Semantic (file-aware) CDC for OCI layer blobs")
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_chunk = sub.add_parser("chunk", help="Chunk blobs with file-semantic mode")
    ap_chunk.add_argument("oci_root", type=Path, help="OCI dir root with blobs/sha256/*")
    ap_chunk.add_argument("-o", "--out-dir", type=Path, default=Path("./semantic_output"))
    ap_chunk.add_argument("--min", type=int, default=32*1024)
    ap_chunk.add_argument("--avg", type=int, default=64*1024)
    ap_chunk.add_argument("--max", type=int, default=128*1024)
    ap_chunk.add_argument("--small-cutover", type=int, default=128*1024, help="<= cutover uses whole-file CAS")

    ap_repack = sub.add_parser("repack", help="Re-pack a single blob from metadata")
    ap_repack.add_argument("meta", type=Path, help="metadata.json path from chunk step")
    ap_repack.add_argument("--blob-name", type=str, required=True, help="prefix match of blob filename to repack")
    ap_repack.add_argument("-o", "--out-tar", type=Path, default=Path("./repacked_layer.tar"))

    ap_repack_all = sub.add_parser("repack-all", help="Re-pack ALL blobs to a directory and report throughput")
    ap_repack_all.add_argument("meta", type=Path, help="metadata.json path from chunk step")
    ap_repack_all.add_argument("-o", "--out-root", type=Path, default=Path("./semantic_repacked"))

    ap_verify = sub.add_parser("verify", help="Verify repacked tar vs decompressed original tar per layer")
    ap_verify.add_argument("oci_root", type=Path, help="original OCI dir root")
    ap_verify.add_argument("repacked_root", type=Path, help="output root from repack-all")

    args = ap.parse_args()

    # Dispatch
    if getattr(args, 'cmd', None) == 'repack-all':
        cmd_repack_all(args.meta.resolve(), args.out_root.resolve())
        return
    if getattr(args, 'cmd', None) == 'verify':
        cmd_verify_semantic(args.oci_root.resolve(), args.repacked_root.resolve())
        return

    # ---- chunk command ----
    if hasattr(args, "oci_root"):
        oci_root = args.oci_root.resolve()
        blobs_dir = oci_root / "blobs" / "sha256"
        out_dir = args.out_dir.resolve()
        chunks_root = out_dir / "chunks"
        work_root = out_dir / "work"
        out_dir.mkdir(parents=True, exist_ok=True)
        chunks_root.mkdir(parents=True, exist_ok=True)
        work_root.mkdir(parents=True, exist_ok=True)

        # Performance counters
        input_total = 0  # original blob bytes considered
        input_total_decomp = 0  # decompressed tar bytes
        total_files = total_dirs = total_syms = total_others = 0
        total_chunks = 0
        sum_chunk_sizes = 0
        min_chunk = None
        max_chunk = 0
        dup_total = 0
        wholefile_cnt = 0

        records: List[LayerRecord] = []
        t0 = time.perf_counter()
        layer_set = oci_layer_digests_from_index(oci_root)
        for p in sorted(blobs_dir.iterdir()):
            if not p.is_file():
                continue
            if layer_set and p.name not in layer_set:
                print(f"[-] skip non-layer blob {p.name} (filtered by manifest)")
                continue
            input_total += p.stat().st_size

            # Decompress to temp and verify it's a tar before processing
            tmp_tar = work_root / (p.name + ".tar")
            stream_decompress_to_file(p, tmp_tar)
            if tmp_tar.exists():
                input_total_decomp += tmp_tar.stat().st_size
            if not detect_tarfile(tmp_tar):
                print(f"[-] skip non-tar blob {p.name} (size={p.stat().st_size}B)")
                tmp_tar.unlink(missing_ok=True)
                continue

            # Process using the already decompressed tar to avoid double work
            rec = process_layer_semantic(
                p, work_root, chunks_root,
                args.min, args.avg, args.max, args.small_cutover,
                pre_decompressed_tar=tmp_tar
            )
            records.append(asdict(rec))
            print(f"[+] processed blob {p.name}: file-entries={len(rec.files)}")

            # accumulate stats
            for f in rec.files:
                if f.type == 'reg':
                    total_files += 1
                    total_chunks += len(f.chunks)
                    for c in f.chunks:
                        sum_chunk_sizes += c.length
                        if (min_chunk is None) or (c.length < min_chunk):
                            min_chunk = c.length
                        if c.length > max_chunk:
                            max_chunk = c.length

                    dup_total += f.duplication
                elif f.type == 'dir':
                    total_dirs += 1
                elif f.type == 'sym':
                    total_syms += 1
                else:
                    total_others += 1

            # Optionally keep tmp_tar for inspection; otherwise uncomment next line to remove
            # tmp_tar.unlink(missing_ok=True)

        meta = {"oci_root": str(oci_root), "chunks_root": str(chunks_root), "records": records}
        (out_dir / "metadata.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

        elapsed = time.perf_counter() - t0
        avg_chunk = (sum_chunk_sizes/total_chunks) if total_chunks else 0
        thr_mb_s = (input_total_decomp/1_048_576)/elapsed if elapsed>0 else 0
        print("--- 성능 (semantic chunk) ---")
        print(f"  고려한 blob(원본 압축) 총 크기: {input_total} B")
        print(f"  디컴프된 tar 총 크기: {input_total_decomp} B")
        print(f"  파일/디렉터리/심볼릭/기타: {total_files}/{total_dirs}/{total_syms}/{total_others}")
        print(f"  총 chunk 개수: {total_chunks}")
        print(f"  평균/최소/최대 chunk 크기: {avg_chunk:.1f} / {0 if min_chunk is None else min_chunk} / {max_chunk} B")
        print(f"  중복으로 저장하지 않은 chunk 개수(대략): {dup_total}")
        print(f"  총 시간: {elapsed:.3f} s  처리량(디컴프 기준): {thr_mb_s:.2f} MB/s")
        print(f"[=] wrote {out_dir/'metadata.json'}")

    else:
        # single-layer repack
        meta = json.loads(args.meta.read_text(encoding="utf-8"))
        chunks_root = Path(meta["chunks_root"])
        rec = None
        for r in meta["records"]:
            if r["blob_name"].startswith(args.blob_name):
                rec = r; break
        if rec is None:
            raise SystemExit(f"blob {args.blob_name} not found in metadata")
        files = [FileEntry(**f) for f in rec["files"]]
        lr = LayerRecord(blob_name=rec["blob_name"], blob_sha256=rec["blob_sha256"], method=rec["method"],
                         params=rec["params"], files=files)
        pack_semantic_to_tar(lr, chunks_root, args.out_tar)
        print(f"[=] wrote {args.out_tar}")

if __name__ == "__main__":
    main()