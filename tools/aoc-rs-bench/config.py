#!/usr/bin/env python3

import argparse
import asyncio
import hashlib
import json
import math
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import TypedDict


class BuildPuzzleDetails(TypedDict):
    uses_threads: bool
    multiversion: list[str]


class MatrixBuilder:
    BUILDS = ["generic", "native"]

    def __init__(self, data_dir: Path, inputs_dir: Path):
        self.data_dir = data_dir
        self.inputs_dir = inputs_dir
        self.builds_dir = data_dir / "builds"
        self.required_inputs = {}
        self.input_cache = {}
        self.total_combinations = 0

        # self.cache[commit][kind][year][day] = details
        self.cache_file = self.builds_dir / ".cache.json"
        if self.cache_file.is_file():
            self.cache = json.loads(self.cache_file.read_text(), object_pairs_hook=nested_defaultdict_hook)
        else:
            self.cache = nested_defaultdict()

    def build(self):
        commits = self.load_commits()
        self.check_binaries(commits)
        results = asyncio.run(self.process_all(commits))

        os.makedirs(self.data_dir / "inputs", exist_ok=True)
        for input_hash, input_bytes in self.required_inputs.items():
            write_atomic(self.data_dir / "inputs" / input_hash, True, lambda f: f.write(input_bytes))

        write_atomic(self.data_dir / "config.json", False, lambda f: json.dump(results, f, indent=2))

        write_atomic(self.cache_file, False, lambda f: json.dump(self.cache, f, indent=2))

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
            for kind in MatrixBuilder.BUILDS:
                f = self.builds_dir / kind / commit
                if not f.is_file():
                    missing.append(str(f))

        if missing:
            raise RuntimeError(f"error: missing builds:\n{"\n".join("  " + s for s in missing)}")

    async def process_all(self, commits: list[str]):
        sem = asyncio.Semaphore(8)

        results = {
            "config_keys": {
                "commit": {
                    "values": commits,
                    "presets": {}
                },
                "build": {
                    "values": MatrixBuilder.BUILDS,
                    "presets": {
                        "all": MatrixBuilder.BUILDS,
                    },
                },
                "threads": {
                    "values": ["1", "2", "4", "n"],
                    "presets": {
                        "multi": ["1", "2", "4", "n"],
                    }
                },
                "multiversion": {
                    "values": ["default"],
                    "presets": {
                        "default": ["default"],
                    }
                }
            },
            "benchmarks": nested_defaultdict(),
        }

        tasks = []
        for commit in commits:
            for kind in MatrixBuilder.BUILDS:
                tasks.append(self.process_build(commit, kind, sem, results))

        # Use return_exceptions=True to avoid built-in cancellation
        task_results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in task_results:
            if isinstance(r, Exception):
                raise r

        post_processed = []
        for key in sorted(results["benchmarks"].keys()):
            post_processed.extend(self.post_process_benchmark(key, results["benchmarks"][key], results["config_keys"]))
        results["benchmarks"] = post_processed

        return results

    async def process_build(self, commit: str, kind: str, sem: asyncio.Semaphore, results: dict):
        async with sem:
            puzzles = await self.get_build_puzzles(commit, kind)
            for puzzle in puzzles:
                details = await self.get_build_puzzle_details(commit, kind, puzzle)
                for v in details["multiversion"]:
                    if v not in results["config_keys"]["multiversion"]["values"]:
                        results["config_keys"]["multiversion"]["values"].append(v)

                results["benchmarks"][f"{puzzle[0]} {puzzle[1]}"][commit] = details

    def post_process_benchmark(self, name: str, benchmark: dict, config_keys: dict) -> list[dict]:
        benchmarks = []
        year, day = name.split(" ")

        all_commits = config_keys["commit"]["values"]
        commits_reversed = [c for c in reversed(all_commits) if c in benchmark]
        while commits_reversed:
            last_commit = commits_reversed.pop()
            chunk_commits = [last_commit]
            chunk_metadata = benchmark[last_commit]

            while commits_reversed and benchmark[commits_reversed[-1]] == chunk_metadata:
                chunk_commits.append(commits_reversed.pop())

            combinations = [
                len(chunk_commits),
                len(config_keys["build"]["presets"]["all"]),
                len(config_keys["threads"]["presets"]["multi"]) if chunk_metadata["uses_threads"] else 1,
                len(chunk_metadata["multiversion"]),
            ]
            self.total_combinations += math.prod(combinations)

            if chunk_commits == all_commits[-len(chunk_commits):]:
                preset_name = f"from_{chunk_commits[0]}"
                config_keys["commit"]["presets"][preset_name] = chunk_commits
                chunk_commits = preset_name

            benchmarks.append({
                "benchmark": name,
                "command": "builds/{build}/{commit} bench " + year + " " + day + " {threads} {multiversion}",
                "input": self.get_input_hash(year, day),
                "checksum": chunk_metadata["checksum"],
                "config": {
                    "commit": chunk_commits,
                    "build": "all",
                    "threads": ("multi" if chunk_metadata["uses_threads"] else ["1"]),
                    "multiversion": chunk_metadata["multiversion"],
                }
            })
        return benchmarks

    async def get_build_puzzles(self, commit: str, kind: str) -> list[tuple[str, str]]:
        cache = self.cache[commit][kind]
        if not cache:
            build = str(self.build_path(commit, kind))
            puzzles_code, puzzles_stdout, puzzles_stderr = await run(build, "puzzles")

            if puzzles_code != 0 or puzzles_stderr != "":
                raise RuntimeError(f"error: gathering puzzles from {build} failed with exit code {puzzles_code}")

            for line in puzzles_stdout.splitlines():
                if re.fullmatch(r"\d{4} \d{2}", line):
                    year, day = line.split(" ")
                    _unused = cache[year][day]
                else:
                    raise RuntimeError(f"error: unexpected output from {build} puzzles: {line}")

        return [(year, day) for year in cache for day in cache[year]]

    async def get_build_puzzle_details(self, commit: str, kind: str, puzzle: tuple[str, str]) -> BuildPuzzleDetails:
        year, day = puzzle
        cache = self.cache[commit][kind][year][day]
        if not cache:
            build = str(self.build_path(commit, kind))
            input_hash = self.get_input_hash(year, day)

            code, stdout, stderr, trace = await strace_run(build, "check", year, day,
                                                           stdin=self.required_inputs[input_hash],
                                                           strace_filter="trace=clone,clone3,fork,vfork")
            if code != 0 or stderr != "":
                raise RuntimeError(f"error: running {build} on {puzzle} failed with exit code {code}:\n{stderr}")

            matches = re.fullmatch(r'^Checksum: ([a-f0-9]+)\r?\nMultiversion: ([a-zA-z0-9_,]+)\r?\n?$', stdout)
            if not matches:
                raise RuntimeError(f"error: unexpected output from {build} on {puzzle}: {stdout}")

            cache["checksum"] = matches.group(1)
            cache["multiversion"] = matches.group(2).split(",")
            cache["uses_threads"] = trace != b''
        return cache

    def get_input_hash(self, year: str, day: str) -> str:
        if (year, day) in self.input_cache:
            return self.input_cache[(year, day)]

        input_file = self.inputs_dir / f"year{year}" / f"day{day}.txt"
        input_bytes = input_file.read_bytes()
        input_hash = hashlib.sha256(input_bytes).hexdigest()

        self.input_cache[(year, day)] = input_hash
        self.required_inputs[input_hash] = input_bytes

        return input_hash

    def build_path(self, commit: str, kind: str) -> Path:
        return self.builds_dir / kind / commit


def write_atomic(file: Path, binary: bool, write_fn):
    tmp_file = file.with_suffix(".tmp")
    with open(tmp_file, "wb" if binary else "w") as f:
        write_fn(f)
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
        stdout, stderr = await asyncio.wait_for(proc.communicate(input=stdin), timeout=30)
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


def main():
    p = argparse.ArgumentParser()
    p.add_argument("data", type=Path, help="Data directory containing a builds directory")
    p.add_argument("inputs", type=Path, help="Directory to copy puzzle inputs from")
    args = p.parse_args()

    builder = MatrixBuilder(args.data, args.inputs)
    builder.build()
    print("total combinations:", builder.total_combinations, file=sys.stderr)


if __name__ == "__main__":
    main()
