#!/usr/bin/env python3

import argparse
import asyncio
import hashlib
import io
import json
import math
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import TypedDict


class BuildPuzzleDetails(TypedDict):
    checksum: str
    uses_threads: bool
    multiversion: list[str]
    all_multiversion: list[str]


class MatrixBuilder:
    DEFAULT_BUILDS = ["generic", "native"]
    KNOWN_BUILDS = {"generic", "native", "generic_lto", "native_lto"}
    DEFAULT_THREADS = ["1", "2", "4", "n"]
    KNOWN_THREADS = set(DEFAULT_THREADS)

    def __init__(
            self,
            data_dir: Path,
            inputs_dir: Path,
            builds: list[str],
            threads: list[str],
            multiversion: list[str] | None,
    ):
        self.data_dir = data_dir
        self.inputs_dir = inputs_dir
        self.builds_dir = data_dir / "builds"
        self.builds = builds
        self.threads = threads
        self.multi_threads = [thread for thread in threads if thread != "1"]
        self.multiversion = set(multiversion) if multiversion is not None else None
        self.seen_multiversions = set()
        self.required_inputs = {}
        self.input_cache = {}
        self.total_combinations = 0
        # Builds are hardlinked when identical, so a binary only needs checking once per input
        self.check_tasks: dict[tuple[int, int, str], asyncio.Task[BuildPuzzleDetails]] = {}

        # self.cache[commit][kind][year][day] = details
        self.cache_file = self.builds_dir / ".cache.json"
        if self.cache_file.is_file():
            self.cache = json.loads(self.cache_file.read_text(), object_pairs_hook=nested_defaultdict_hook)
        else:
            self.cache = nested_defaultdict()

    def build(self):
        commits = self.load_commits()
        self.check_binaries(commits)
        try:
            results = asyncio.run(self.process_all(commits))
        finally:
            # Entries are only added once validated, so what a failed run finished is safe to keep
            write_atomic(self.cache_file, False, lambda f: json.dump(self.cache, f, indent=2))

        os.makedirs(self.data_dir / "inputs", exist_ok=True)
        for input_hash, input_bytes in self.required_inputs.items():
            write_atomic(self.data_dir / "inputs" / input_hash, True, lambda f: f.write(input_bytes))

        write_atomic(self.data_dir / "config.json", False, lambda f: json.dump(results, f, indent=2))

    def load_commits(self):
        commits_file = self.builds_dir / "commits.txt"
        if not commits_file.is_file():
            raise RuntimeError(f"error: missing {commits_file}")

        commits = []
        with commits_file.open("r", encoding="utf-8") as f:
            for line in f:
                commit = line.strip()
                if commit:
                    commits.append(commit)
        if not commits:
            raise RuntimeError(f"error: {commits_file} is empty")

        return commits

    def check_binaries(self, commits: list[str]):
        missing = []

        for commit in commits:
            for kind in self.builds:
                f = self.builds_dir / kind / commit / "2015-01"
                if not f.is_file():
                    missing.append(str(f))

        if missing:
            missing_lines = "\n".join("  " + s for s in missing)
            raise RuntimeError(f"error: missing builds:\n{missing_lines}")

    async def process_all(self, commits: list[str]):
        # Threaded puzzles use every core, so running more than this at once only risks timeouts on slow hosts
        sem = asyncio.Semaphore(len(os.sched_getaffinity(0)))

        multiversion_values = []
        multiversion_presets = {}
        if self.multiversion is None or "default" in self.multiversion:
            multiversion_values.append("default")
            multiversion_presets["default"] = ["default"]

        thread_presets = {}
        if self.multi_threads:
            thread_presets["multi"] = self.multi_threads

        results = {
            "config_keys": {
                "commit": {
                    "link": "https://github.com/ictrobot/aoc-rs/commit/{value}",
                    "values": commits,
                    "presets": {}
                },
                "build": {
                    "values": self.builds,
                    "presets": {
                        "all": self.builds,
                    },
                    "metric_label": True,
                },
                "threads": {
                    "values": self.threads,
                    "presets": thread_presets,
                    "metric_label": True,
                },
                "multiversion": {
                    "values": multiversion_values,
                    "presets": multiversion_presets,
                    "metric_label": True,
                }
            },
            "benchmarks": nested_defaultdict(),
        }

        tasks = []
        for commit in commits:
            for kind in self.builds:
                tasks.append(self.process_build(commit, kind, sem, results))

        # Use return_exceptions=True to avoid built-in cancellation
        task_results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in task_results:
            if isinstance(r, Exception):
                raise r

        # Reorder multiversion values: use the last commit's 2015-04 generic build ordering as canonical
        # (from cache, so no subprocess is spawned), then append any values from earlier commits
        # that are no longer present
        last_details = await self.get_build_puzzle_details(commits[-1], self.builds[0], ("2015", "04"), sem)
        last_mv = last_details["multiversion"] if last_details is not None else []
        seen = results["config_keys"]["multiversion"]["values"]
        results["config_keys"]["multiversion"]["values"] = last_mv + [v for v in seen if v not in last_mv]

        annotations = await self.generate_commit_annotations(commits, sem)
        if annotations:
            results["config_keys"]["commit"]["annotations"] = annotations

        post_processed = []
        for key in sorted(results["benchmarks"].keys()):
            post_processed.append(self.post_process_benchmark(key, results["benchmarks"][key], results["config_keys"]))
        results["benchmarks"] = post_processed

        return results

    async def process_build(self, commit: str, kind: str, sem: asyncio.Semaphore, results: dict):
        puzzles = self.get_build_puzzles(commit, kind)
        for puzzle in puzzles:
            details = await self.get_build_puzzle_details(commit, kind, puzzle, sem)
            if details is None:
                continue
            for v in details["multiversion"]:
                if v not in results["config_keys"]["multiversion"]["values"]:
                    results["config_keys"]["multiversion"]["values"].append(v)

            # Compiled but unsupported versions must not split otherwise identical commit groups.
            results["benchmarks"][f"{puzzle[0]}-{puzzle[1]}"][commit] = {
                k: v for k, v in details.items() if k != "all_multiversion"
            }

    def post_process_benchmark(self, name: str, benchmark: dict, config_keys: dict) -> dict:
        variants = []
        year, day = name.split("-")

        all_commits = config_keys["commit"]["values"]
        commits_reversed = [c for c in reversed(all_commits) if c in benchmark]
        while commits_reversed:
            last_commit = commits_reversed.pop()
            chunk_commits = [last_commit]
            chunk_metadata = benchmark[last_commit]

            while commits_reversed and benchmark[commits_reversed[-1]] == chunk_metadata:
                chunk_commits.append(commits_reversed.pop())

            multiversion_values = chunk_metadata["multiversion"]
            remaining_multiversion = [v for v in multiversion_values if v != "default"]

            # The default multiversion gets every thread count. Explicit multiversions only get 1
            # and n: the intermediate counts scale the same way for every version, so they add
            # measurement time without telling anything new
            default_thread_groups = [["1"]]
            explicit_thread_groups = [["1"]]
            if chunk_metadata["uses_threads"]:
                if self.multi_threads:
                    default_thread_groups.append("multi")
                if "n" in self.multi_threads:
                    explicit_thread_groups.append(["n"])

            default_cases = len(self.threads) if chunk_metadata["uses_threads"] else 1
            explicit_cases = len(explicit_thread_groups) * len(remaining_multiversion)
            combinations = [
                len(chunk_commits),
                len(config_keys["build"]["presets"]["all"]),
                (default_cases if "default" in multiversion_values else 0) + explicit_cases,
            ]
            self.total_combinations += math.prod(combinations)

            if chunk_commits == all_commits[-len(chunk_commits):]:
                preset_name = f"from_{chunk_commits[0]}"
                config_keys["commit"]["presets"][preset_name] = chunk_commits
                chunk_commits = preset_name

            def add_variant(multiversion, thread_groups):
                for threads in thread_groups:
                    variants.append({
                        "checksum": chunk_metadata["checksum"],
                        "config": {
                            "commit": chunk_commits,
                            "build": "all",
                            "threads": threads,
                            "multiversion": multiversion,
                        },
                    })

            if "default" in multiversion_values:
                add_variant("default", default_thread_groups)

            if remaining_multiversion:
                add_variant(remaining_multiversion, explicit_thread_groups)

        return {
            "benchmark": name,
            "dedupe": "inode-content",
            "command": [
                f"builds/{{build}}/{{commit}}/{year}-{day}",
                "bench",
                "{threads}",
                "{multiversion}",
            ],
            "input": self.get_input_hash(year, day),
            **(
                variants[0] if len(variants) == 1 else {
                    "variants": variants,
                }
            )
        }

    def get_build_puzzles(self, commit: str, kind: str) -> list[tuple[str, str]]:
        build_dir = self.commit_build_dir(commit, kind)

        puzzles = []
        for filename in os.listdir(build_dir):
            if re.fullmatch(r"\d{4}-\d{2}", filename):
                year, day = filename.split("-")
                puzzles.append((year, day))
            else:
                raise RuntimeError(f"error: unexpected filename under {build_dir}: {filename}")

        return puzzles

    async def get_build_puzzle_details(self, commit: str, kind: str, puzzle: tuple[str, str],
                                       sem: asyncio.Semaphore) -> BuildPuzzleDetails | None:
        year, day = puzzle
        cache = self.cache[commit][kind][year][day]
        if not cache:
            build = self.build_path(commit, kind, puzzle)
            input_hash = self.get_input_hash(year, day)

            stat = build.stat()
            key = (stat.st_dev, stat.st_ino, input_hash)
            if key not in self.check_tasks:
                self.check_tasks[key] = asyncio.create_task(self.check_binary(str(build), input_hash, puzzle, sem))
            cache.update(await self.check_tasks[key])

        for version in cache["multiversion"]:
            self.seen_multiversions.add(version)

        if self.multiversion is None:
            return cache

        multiversion = [v for v in cache["multiversion"] if v in self.multiversion]
        if not multiversion:
            return None
        details = cache.copy()
        details["multiversion"] = multiversion
        return details

    async def check_binary(self, build: str, input_hash: str, puzzle: tuple[str, str],
                           sem: asyncio.Semaphore) -> BuildPuzzleDetails:
        async with sem:
            code, stdout, stderr, trace = await strace_run(build, "check",
                                                           stdin=self.required_inputs[input_hash],
                                                           strace_filter="trace=clone,clone3,fork,vfork")
        if code != 0 or stderr != "":
            raise RuntimeError(f"error: running {build} on {puzzle} failed with exit code {code}:\n{stderr}")

        matches = re.fullmatch(
            r'^Checksum: ([a-f0-9]+)\r?\n'
            r'Multiversion: ([a-zA-Z0-9_,]+)\r?\n'
            r'MultiversionAll: ([a-zA-Z0-9_,]+)\r?\n'
            r'Multithreading: (true|false)\r?\n?$',
            stdout)
        if not matches:
            raise RuntimeError(f"error: unexpected output from {build} on {puzzle}: {stdout}")

        uses_threads = trace != b''
        multithreading = matches.group(4) == "true"
        if multithreading != uses_threads:
            raise RuntimeError(f"error: {build} on {puzzle} was built "
                               f"{'with' if multithreading else 'without'} multithreading, but "
                               f"{'did not create' if multithreading else 'created'} threads")

        return {
            "checksum": matches.group(1),
            "multiversion": matches.group(2).split(","),
            "all_multiversion": matches.group(3).split(","),
            "uses_threads": uses_threads,
        }

    def get_input_hash(self, year: str, day: str) -> str:
        if (year, day) in self.input_cache:
            return self.input_cache[(year, day)]

        input_file = self.inputs_dir / f"year{year}" / f"day{day}.txt"
        input_bytes = input_file.read_bytes()
        input_hash = hashlib.sha256(input_bytes).hexdigest()

        self.input_cache[(year, day)] = input_hash
        self.required_inputs[input_hash] = input_bytes

        return input_hash

    def commit_build_dir(self, commit: str, kind: str) -> Path:
        return self.builds_dir / kind / commit

    def build_path(self, commit: str, kind: str, puzzle: tuple[str, str]) -> Path:
        return self.commit_build_dir(commit, kind) / f"{puzzle[0]}-{puzzle[1]}"

    async def get_rust_version(self, commit: str, sem: asyncio.Semaphore) -> str | None:
        cached = self.cache[commit].get("rust_version")
        if cached:
            return cached

        binary = self.build_path(commit, self.builds[0], ("2015", "01"))
        if not binary.is_file():
            return None

        async with sem:
            code, stdout, stderr = await run("readelf", "-p", ".comment", str(binary))

        if code != 0:
            return None

        match = re.search(r"rustc version (\d+\.\d+\.\d+)", stdout)
        if match:
            self.cache[commit]["rust_version"] = match.group(1)
            return match.group(1)
        return None

    async def generate_commit_annotations(
            self, commits: list[str], sem: asyncio.Semaphore
    ) -> dict[str, str]:
        versions = await asyncio.gather(
            *(self.get_rust_version(c, sem) for c in commits)
        )

        annotations = {}
        prev_version = None
        for commit, version in zip(commits, versions):
            if version is not None and version != prev_version:
                if prev_version is not None:
                    annotations[commit] = f"Rust {version}"
            prev_version = version if version is not None else prev_version

        return annotations


def write_atomic(file: Path, binary: bool, write_fn):
    # Render in memory first and skip the write if the file already has identical content
    buf = io.BytesIO() if binary else io.StringIO()
    write_fn(buf)
    data = buf.getvalue()
    if not binary:
        data = data.encode("utf-8")

    try:
        if file.stat().st_size == len(data) and file.read_bytes() == data:
            return
    except OSError:
        pass

    tmp_file = file.with_suffix(".tmp")
    tmp_file.write_bytes(data)
    os.replace(tmp_file, file)


async def run(*cmd: str):
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise RuntimeError(f"error: {cmd} timed out")

    return proc.returncode, stdout.decode(), stderr.decode()


async def strace_run(*cmd: str, stdin: bytes = None, strace_filter: str = None):
    rfd, wfd = os.pipe()
    os.set_blocking(rfd, False)

    cmd = ["strace", "-o", "/dev/fd/" + str(wfd), "-c"] + (["-e", strace_filter] if strace_filter else []) + [
        "--"] + list(cmd)

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE if stdin else asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        pass_fds=[wfd]
    )

    # Don't need to write in the parent
    os.close(wfd)

    # Wrap rfd in an asyncio StreamReader
    loop = asyncio.get_running_loop()
    trace_reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(trace_reader)
    pipe = os.fdopen(rfd, "rb", buffering=0)
    transport, _ = await loop.connect_read_pipe(lambda: protocol, pipe)
    trace_task = asyncio.create_task(trace_reader.read())

    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(input=stdin), timeout=120)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise RuntimeError(f"error: {cmd} timed out")

    trace = await trace_task

    return proc.returncode, stdout.decode(), stderr.decode(), trace


def nested_defaultdict():
    return defaultdict(nested_defaultdict)


def nested_defaultdict_hook(pairs):
    d = nested_defaultdict()
    for k, v in pairs:
        d[k] = v
    return d


def csv_env(
        name: str,
        default: list[str] | None = None,
) -> list[str] | None:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return list(default) if default is not None else None
    return sorted({part.strip() for part in raw.split(",")})


def main():
    p = argparse.ArgumentParser()
    p.add_argument("data", type=Path, help="Data directory containing a builds directory")
    p.add_argument("inputs", type=Path, help="Directory to copy puzzle inputs from")
    args = p.parse_args()

    builds = csv_env("AOC_BENCH_PROFILES", MatrixBuilder.DEFAULT_BUILDS)
    assert builds is not None
    unknown_builds = set(builds) - MatrixBuilder.KNOWN_BUILDS
    if unknown_builds:
        raise RuntimeError(f"error: unknown build profiles: {', '.join(sorted(unknown_builds))}")

    threads = csv_env("AOC_BENCH_THREADS", MatrixBuilder.DEFAULT_THREADS)
    assert threads is not None
    unknown_threads = set(threads) - MatrixBuilder.KNOWN_THREADS
    if unknown_threads:
        raise RuntimeError(f"error: unknown thread values: {', '.join(sorted(unknown_threads))}")
    if "1" not in threads:
        raise RuntimeError("error: thread values must include 1")

    multiversion = csv_env("AOC_BENCH_MULTIVERSION")

    builder = MatrixBuilder(args.data, args.inputs, builds, threads, multiversion)
    builder.build()

    if multiversion is not None:
        unknown_multiversions = set(multiversion) - builder.seen_multiversions
        if unknown_multiversions:
            raise RuntimeError(
                f"error: unknown multiversion values: {', '.join(sorted(unknown_multiversions))}"
            )

    print("total combinations:", builder.total_combinations, file=sys.stderr)


if __name__ == "__main__":
    main()
