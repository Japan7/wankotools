#!/usr/bin/env python
import asyncio
import datetime
import logging
import os
from pathlib import Path
from typing import Annotated, Any, Coroutine, Iterable

import aiofiles
import backoff
import httpcore
import httpx
import pydantic
import typer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


download_backoff = backoff.on_exception(
    backoff.expo,
    (httpcore.ReadTimeout, httpx.ReadTimeout),
    max_time=150,
)


class KaraberusKara(pydantic.BaseModel):
    ID: int
    VideoUploaded: bool
    VideoModTime: datetime.datetime
    SubtitlesUploaded: bool
    SubtitlesModTime: datetime.datetime
    InstrumentalUploaded: bool
    InstrumentalModTime: datetime.datetime


class KaraberusKarasResponse(pydantic.BaseModel):
    Karas: list[KaraberusKara]


class KaraberusClient:
    def __init__(self, client: httpx.AsyncClient, base_url: str, base_dir: Path):
        self.base_url = base_url.rstrip("/")
        self.client = client
        self.base_dir = base_dir
        self.init_base_dir()

    def init_base_dir(self):
        self.base_dir.mkdir(exist_ok=True, parents=True)
        gitignore = self.base_dir / ".gitignore"
        if not gitignore.exists():
            gitignore.write_text("*")

    async def get_karas(self) -> list[KaraberusKara]:
        kara_endpoint = f"{self.base_url}/api/kara"

        resp = await self.client.get(kara_endpoint)
        resp.raise_for_status()
        karas = KaraberusKarasResponse.model_validate(resp.json())
        return karas.Karas

    @download_backoff
    async def _download(self, filename: Path, resp: httpx.Response, ts: float):
        resp.raise_for_status()
        logger.info(f"downloading {filename}")
        try:
            async with aiofiles.open(filename, "wb") as f:
                for data in resp.iter_bytes(1024 * 1024):
                    await f.write(data)
        except Exception:
            await asyncio.to_thread(filename.unlink)
            raise

        # add 1 second just in case
        os.utime(filename, (ts + 1, ts + 1))

    async def download(self, kara: KaraberusKara) -> None:
        kid = kara.ID
        vid_filename = self.base_dir / f"{kid}.mkv"
        try:
            vid_stat = await asyncio.to_thread(vid_filename.stat)
            vid_mtime = vid_stat.st_mtime
        except FileNotFoundError:
            vid_mtime = 0
        vid_ts = kara.VideoModTime.timestamp()
        if kara.VideoUploaded and vid_ts > vid_mtime:
            endpoint = f"{self.base_url}/api/kara/{kid}/download/video"
            resp = await self.client.get(endpoint)
            await self._download(vid_filename, resp, vid_ts)

        sub_filename = self.base_dir / f"{kid}.ass"
        try:
            sub_stat = await asyncio.to_thread(sub_filename.stat)
            sub_mtime = sub_stat.st_mtime
        except FileNotFoundError:
            sub_mtime = 0

        sub_ts = kara.SubtitlesModTime.timestamp()
        if kara.SubtitlesUploaded and sub_ts > sub_mtime:
            endpoint = f"{self.base_url}/api/kara/{kid}/download/sub"
            resp = await self.client.get(endpoint)
            await self._download(sub_filename, resp, sub_ts)

        inst_filename = self.base_dir / f"{kid}.mka"
        try:
            inst_stat = await asyncio.to_thread(inst_filename.stat)
            inst_mtime = inst_stat.st_mtime
        except FileNotFoundError:
            inst_mtime = 0

        inst_ts = kara.InstrumentalModTime.timestamp()
        if kara.InstrumentalUploaded and inst_ts > inst_mtime:
            endpoint = f"{self.base_url}/api/kara/{kid}/download/inst"
            resp = await self.client.get(endpoint)
            await self._download(inst_filename, resp, inst_ts)


class DownloadRunner:
    def __init__(self, coros: Iterable[Coroutine[Any, Any, None]]) -> None:
        self.queue: asyncio.Queue[Coroutine[Any, Any, None]] = asyncio.Queue()
        for coro in coros:
            self.queue.put_nowait(coro)

    async def run(self):
        try:
            while True:
                coro = self.queue.get_nowait()
                await coro
        except asyncio.QueueEmpty:
            pass
        except Exception as e:
            logger.exception(e)
            raise


async def sync(base_url: str, token: str, dest_dir: Path, parallel: int):
    headers = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient(headers=headers) as hclient:
        client = KaraberusClient(hclient, base_url, dest_dir)
        karas = await client.get_karas()

        downloads = DownloadRunner(client.download(kara) for kara in karas)
        async with asyncio.TaskGroup() as t:
            for _ in range(parallel):
                t.create_task(downloads.run())


def main(
    base_url: str,
    token: str,
    dest_dir: Annotated[
        Path, typer.Option("--directory", "-d", file_okay=False, dir_okay=True)
    ] = Path("."),
    parallel: Annotated[int, typer.Option("--parallel", "-p")] = 4,
):
    asyncio.run(sync(base_url, token, dest_dir, parallel))


def wankosync():
    typer.run(main)


if __name__ == "__main__":
    wankosync()
