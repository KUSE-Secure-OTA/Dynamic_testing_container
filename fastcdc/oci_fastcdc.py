#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OCI 디렉터리(oci-archive를 tar -xf로 푼 폴더)에서 blobs/sha256/* 파일들을
FastCDC로 청크 분할/복원/검증/패킹하는 유틸리티. 모든 청크는 fastcdc_output/chunks 아래에 <sha256>.bin으로 저장되어 중복이 자동 제거됩니다.

사용 예시는 파일 끝의 주석 참조.
"""

import argparse
import hashlib
import json
import tarfile
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Tuple, Any
from fastcdc import fastcdc  # pip install fastcdc


BLOBS_SUBDIR = Path("blobs/sha256")


def sha256_file(p: Path, bufsize: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        while True:
            b = f.read(bufsize)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


@dataclass
class ChunkRec:
    file: str       # 청크 파일 이름(디렉터리 없이, chunks 루트에 직접 저장). 보통 <sha256>.bin
    offset: int
    length: int
    sha256: str


@dataclass
class BlobRec:
    relpath: str    # 예: 'blobs/sha256/abcd...'
    size: int
    sha256: str
    params: Dict[str, int]  # {"min":..,"avg":..,"max":..}
    chunks: List[ChunkRec]


def list_blob_files(oci_root: Path) -> List[Path]:
    blobs_dir = oci_root / BLOBS_SUBDIR
    if not blobs_dir.is_dir():
        raise FileNotFoundError(f"{blobs_dir} 가 존재하지 않습니다.")
    return sorted([p for p in blobs_dir.iterdir() if p.is_file()])



def chunk_one_blob(blob_path: Path, chunks_root: Path,
                   min_size: int, avg_size: int, max_size: int) -> Tuple[BlobRec, int]:
    rel = blob_path.relative_to(blob_path.parents[2])  # oci_root 기준 상대경로

    # FastCDC 경계 탐지
    results = list(fastcdc(str(blob_path), min_size, avg_size, max_size))

    chunks: List[ChunkRec] = []
    size = blob_path.stat().st_size
    dup_skipped = 0

    with blob_path.open("rb") as rf:
        for r in results:
            rf.seek(r.offset)
            data = rf.read(r.length)
            h = hashlib.sha256(data).hexdigest()
            # 모든 청크는 chunks_root 바로 아래에 <sha256>.bin 이름으로 저장 (중복 제거)
            out_name = f"{h}.bin"
            out_path = chunks_root / out_name
            if not out_path.exists():
                with out_path.open("wb") as wf:
                    wf.write(data)
            else:
                dup_skipped += 1
            chunks.append(ChunkRec(
                file=out_name,  # 디렉터리 없이 파일명만 기록
                offset=r.offset, length=r.length, sha256=h
            ))

    blob_sha = sha256_file(blob_path)
    return (BlobRec(
        relpath=str(rel.as_posix()),
        size=size,
        sha256=blob_sha,
        params={"min": min_size, "avg": avg_size, "max": max_size},
        chunks=chunks
    ), dup_skipped)


def cmd_chunk(args: argparse.Namespace) -> None:
    oci_root = Path(args.oci_root).resolve()
    out_dir = Path(args.out_dir).resolve()
    chunks_root = out_dir / "chunks"
    chunks_root.mkdir(parents=True, exist_ok=True)

    # Find blob files
    blob_paths = list_blob_files(oci_root)
    records: List[BlobRec] = []

    # Divide into Chunks
    input_total = sum(p.stat().st_size for p in blob_paths)
    total_chunks = 0
    sum_chunk_sizes = 0
    min_chunk = None
    max_chunk = 0
    dup_total = 0
    print(f"[+] chunk: {len(blob_paths)} blob(s), params(min={args.min}, avg={args.avg}, max={args.max})")
    t0 = time.perf_counter()
    for p in blob_paths:
        rec, dup_skipped = chunk_one_blob(p, chunks_root, args.min, args.avg, args.max)
        dup_total += dup_skipped
        total_chunks += len(rec.chunks)
        for c in rec.chunks:
            sum_chunk_sizes += c.length
            if (min_chunk is None) or (c.length < min_chunk):
                min_chunk = c.length
            if c.length > max_chunk:
                max_chunk = c.length
        print(f"  - {p.name}: size={rec.size} sha256={rec.sha256[:16]}.. chunks={len(rec.chunks)}")
        records.append(rec)
    t1 = time.perf_counter()
    elapsed = t1 - t0
    avg_chunk = (sum_chunk_sizes/total_chunks) if total_chunks else 0
    thr_mb_s = (input_total/1_048_576)/elapsed if elapsed>0 else 0
    print("--- 성능 (cmd_chunk) ---")
    print(f"  입력 파일 총 크기: {input_total} B")
    print(f"  총 chunk 개수: {total_chunks}")
    print(f"  평균 chunk 크기: {avg_chunk:.1f} B")
    print(f"  최소 chunk 크기: {0 if min_chunk is None else min_chunk} B")
    print(f"  최대 chunk 크기: {max_chunk} B")
    print(f"  총 시간: {elapsed:.3f} s")
    print(f"  처리량: {thr_mb_s:.2f} MB/s")
    print(f"  중복으로 저장하지 않은 chunk 개수: {dup_total}")

    meta = {
        "oci_root": str(oci_root),
        "chunks_root": str(chunks_root),
        "blobs": [asdict(r) for r in records],
    }
    (out_dir / "metadata.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[=] wrote metadata: {out_dir / 'metadata.json'}")


def cmd_reconstruct(args: argparse.Namespace) -> None:
    meta_path = Path(args.meta).resolve()
    out_root = Path(args.out_root).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    chunks_root = Path(meta["chunks_root"])

    print(f"[+] reconstruct to: {out_root}")

    total_recon_chunks = 0
    t0 = time.perf_counter()

    for b in meta["blobs"]:
        total_recon_chunks += len(b["chunks"])
        relpath = Path(b["relpath"])
        dest = out_root / relpath
        dest.parent.mkdir(parents=True, exist_ok=True)

        with dest.open("wb") as wf:
            for c in b["chunks"]:
                cpath = chunks_root / c["file"]
                data = cpath.read_bytes()
                # 검증(옵션) — chunk 해시 확인
                if hashlib.sha256(data).hexdigest() != c["sha256"]:
                    raise ValueError(f"청크 해시 불일치: {cpath}")
                wf.write(data)

        # blob 단위 sha256 일치 확인
        rebuilt_sha = sha256_file(dest)
        if rebuilt_sha != b["sha256"]:
            raise ValueError(f"블랍 해시 불일치: {relpath} {rebuilt_sha} != {b['sha256']}")
        print(f"  - rebuilt {relpath.name}: size={dest.stat().st_size} sha256={rebuilt_sha[:16]}..")

    # 재조립 성능 집계
    t1 = time.perf_counter()
    total_bytes = 0
    for b in meta["blobs"]:
        relpath = Path(b["relpath"])
        dest = out_root / relpath
        if dest.exists():
            total_bytes += dest.stat().st_size
    elapsed = t1 - t0
    thr_mb_s = (total_bytes/1_048_576)/elapsed if elapsed>0 else 0
    print("--- 성능 (cmd_reconstruct) ---")
    print(f"  재조립 chunk 개수: {total_recon_chunks}")
    print(f"  재조립한 총 파일 크기: {total_bytes} B")
    print(f"  총 시간: {elapsed:.3f} s")
    print(f"  처리량: {thr_mb_s:.2f} MB/s")

    # index.json, oci-layout 등은 원본에서 복사(바이트 동일성 유지)
    oci_root = Path(meta["oci_root"])
    for extra in ["index.json", "oci-layout"]:
        src = oci_root / extra
        if src.exists():
            dst = out_root / extra
            dst.write_bytes(src.read_bytes())
            print(f"  - copied {extra}")

    print("[=] reconstruct OK (모든 blob sha256 일치)")


def cmd_verify(args: argparse.Namespace) -> None:
    src_root = Path(args.src_root).resolve()
    dst_root = Path(args.dst_root).resolve()

    src_blobs = list_blob_files(src_root)
    dst_blobs = list_blob_files(dst_root)

    src_map = {p.name: p for p in src_blobs}
    dst_map = {p.name: p for p in dst_blobs}

    missing = sorted(set(src_map) - set(dst_map))
    extra = sorted(set(dst_map) - set(src_map))
    if missing:
        print(f"[!] 누락된 blob: {len(missing)}개 -> {missing[:5]}{' ...' if len(missing)>5 else ''}")
    if extra:
        print(f"[!] 추가 blob(예상 외): {len(extra)}개 -> {extra[:5]}{' ...' if len(extra)>5 else ''}")

    ok = 0
    bad = 0
    for name, sp in src_map.items():
        dp = dst_map.get(name)
        if not dp:
            bad += 1
            continue
        s = sha256_file(sp)
        d = sha256_file(dp)
        if s == d:
            ok += 1
        else:
            print(f"[X] 해시 불일치: {name} {s[:16]}.. != {d[:16]}..")
            bad += 1

    print(f"[=] verify: ok={ok}, bad={bad}, total={len(src_map)}")
    if bad > 0:
        raise SystemExit(1)


def reset_tarinfo(info: tarfile.TarInfo) -> tarfile.TarInfo:
    # 재현 가능한 tar를 위해 메타데이터 정규화
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = 0
    return info


def cmd_pack(args: argparse.Namespace) -> None:
    src_root = Path(args.src_root).resolve()
    out_tar = Path(args.out_tar).resolve()
    out_tar.parent.mkdir(parents=True, exist_ok=True)

    # tarfile은 디렉터리 순서를 지정할 수 있으니, 이름순으로 정렬
    members: List[Path] = []
    for p in sorted(src_root.rglob("*")):
        # tar 루트 내 경로는 src_root 기준 상대경로를 사용
        members.append(p)

    print(f"[+] pack {src_root} -> {out_tar} (members={len(members)})")
    with tarfile.open(out_tar, "w") as tf:
        for p in members:
            arcname = p.relative_to(src_root)
            # 파일 메타데이터를 정규화하기 위해 filter 사용
            tf.add(p, arcname=str(arcname), filter=reset_tarinfo)
    print(f"[=] wrote {out_tar} ({out_tar.stat().st_size} bytes)")


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="OCI 이미지 FastCDC 청크 분할/복원/검증/패킹 도구")
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_chunk = sub.add_parser("chunk", help="OCI 디렉터리의 blobs를 FastCDC로 분할")
    ap_chunk.add_argument("oci_root", type=Path, help="tar -xf로 풀어둔 OCI 디렉터리 루트 (예: ./ubuntu-oci)")
    ap_chunk.add_argument("-o", "--out-dir", type=Path, default=Path("./fastcdc_output"),
                          help="메타데이터/청크 출력 디렉터리 (기본: ./fastcdc_output)")
    ap_chunk.add_argument("--min", type=int, default=4 * 1024, help="최소 청크 크기 (기본 4096)")
    ap_chunk.add_argument("--avg", type=int, default=8 * 1024, help="평균 청크 크기 (기본 16384)")
    ap_chunk.add_argument("--max", type=int, default=16 * 1024, help="최대 청크 크기 (기본 65536)")
    ap_chunk.set_defaults(func=cmd_chunk)

    ap_recon = sub.add_parser("reconstruct", help="청크와 메타데이터로 OCI 디렉터리 복원")
    ap_recon.add_argument("meta", type=Path, help="metadata.json 경로 (chunk 단계에서 생성됨)")
    ap_recon.add_argument("-o", "--out-root", type=Path, default=Path("./ubuntu-oci-rebuilt"),
                          help="복원 디렉터리 루트 (기본: ./ubuntu-oci-rebuilt)")
    ap_recon.set_defaults(func=cmd_reconstruct)

    ap_verify = sub.add_parser("verify", help="원본 vs 복원 디렉터리 blob 해시 검증")
    ap_verify.add_argument("src_root", type=Path, help="원본 OCI 디렉터리 루트")
    ap_verify.add_argument("dst_root", type=Path, help="복원 OCI 디렉터리 루트")
    ap_verify.set_defaults(func=cmd_verify)

    ap_pack = sub.add_parser("pack", help="복원 디렉터리를 재현성 있는 tar(oci-archive)로 패킹")
    ap_pack.add_argument("src_root", type=Path, help="패킹할 OCI 디렉터리 루트")
    ap_pack.add_argument("-o", "--out-tar", type=Path, default=Path("./ubuntu_repacked.tar"),
                         help="출력 tar 경로 (기본: ./ubuntu_repacked.tar)")
    ap_pack.set_defaults(func=cmd_pack)

    return ap


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()