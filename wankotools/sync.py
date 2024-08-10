#!/usr/bin/env python
import asyncio
import datetime
import itertools
import logging
import os
import re
from pathlib import Path
from typing import Annotated, Any, Coroutine, Iterable

import aiofiles
import backoff
import httpcore
import httpx
import pydantic
import typer

logger = logging.getLogger(__name__)


class KaraberusIncompleteDownloadError(RuntimeError):
    pass


download_backoff = backoff.on_exception(
    backoff.expo,
    (httpcore.ReadTimeout, httpx.ReadTimeout, KaraberusIncompleteDownloadError),
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


class Font(pydantic.BaseModel):
    ID: int
    Name: str


class KaraberusFontsResponse(pydantic.BaseModel):
    Fonts: list[Font]


content_range_parser = re.compile(r"bytes (\d+)-(\d+)/(\d+)")


class KaraberusClient:
    def __init__(self, client: httpx.AsyncClient, base_url: str, base_dir: Path):
        self.base_url = base_url.rstrip("/")
        self.client = client
        self.base_dir = base_dir
        self.karas_base_dir = base_dir / "karas"
        self.fonts_base_dir = base_dir / "fonts"
        self.init_base_dir()

    def init_base_dir(self):
        self.karas_base_dir.mkdir(exist_ok=True, parents=True)
        self.fonts_base_dir.mkdir(exist_ok=True, parents=True)

        gitignore = self.base_dir / ".gitignore"
        if not gitignore.exists():
            gitignore.write_text("*")

    async def get_karas(self) -> list[KaraberusKara]:
        kara_endpoint = f"{self.base_url}/api/kara"

        resp = await self.client.get(kara_endpoint)
        resp.raise_for_status()
        karas = KaraberusKarasResponse.model_validate(resp.json())
        return karas.Karas

    async def get_fonts(self) -> list[Font]:
        font_endpoint = f"{self.base_url}/api/font"
        resp = await self.client.get(font_endpoint)
        resp.raise_for_status()
        fonts = KaraberusFontsResponse.model_validate(resp.json())
        return fonts.Fonts

    async def _download(self, filename: Path, resp: httpx.Response, ts: float | None):
        resp.raise_for_status()
        logger.info(f"downloading {filename}")
        try:
            content_range_parsed = content_range_parser.match(
                resp.headers["Content-Range"]
            )
            if content_range_parsed is None:
                raise RuntimeError("no Content-Range header")
            expected = int(content_range_parsed.group(3))

            async with aiofiles.open(filename, "wb") as f:
                for data in resp.iter_bytes(1024 * 64):
                    await f.write(data)

                written = await f.tell()
                if written != expected:
                    raise KaraberusIncompleteDownloadError(
                        f"downloaded {written} bytes when file is {expected} bytes"
                    )
        except Exception:
            await asyncio.to_thread(filename.unlink)
            raise

        if ts is not None:
            # add 1 second just in case
            os.utime(filename, (ts + 1, ts + 1))

    @download_backoff
    async def download_url(self, url: str, filename: Path, ts: float | None):
        resp = await self.client.get(url)
        await self._download(filename, resp, ts)

    async def download(self, kara: KaraberusKara) -> None:
        kid = kara.ID
        vid_filename = self.karas_base_dir / f"{kid}.mkv"
        try:
            vid_stat = await asyncio.to_thread(vid_filename.stat)
            vid_mtime = vid_stat.st_mtime
        except FileNotFoundError:
            vid_mtime = 0
        vid_ts = kara.VideoModTime.timestamp()
        if kara.VideoUploaded and vid_ts > vid_mtime:
            endpoint = f"{self.base_url}/api/kara/{kid}/download/video"
            await self.download_url(endpoint, vid_filename, vid_ts)

        sub_filename = self.karas_base_dir / f"{kid}.ass"
        try:
            sub_stat = await asyncio.to_thread(sub_filename.stat)
            sub_mtime = sub_stat.st_mtime
        except FileNotFoundError:
            sub_mtime = 0

        sub_ts = kara.SubtitlesModTime.timestamp()
        if kara.SubtitlesUploaded and sub_ts > sub_mtime:
            endpoint = f"{self.base_url}/api/kara/{kid}/download/sub"
            await self.download_url(endpoint, sub_filename, sub_ts)

        inst_filename = self.karas_base_dir / f"{kid}.mka"
        try:
            inst_stat = await asyncio.to_thread(inst_filename.stat)
            inst_mtime = inst_stat.st_mtime
        except FileNotFoundError:
            inst_mtime = 0

        inst_ts = kara.InstrumentalModTime.timestamp()
        if kara.InstrumentalUploaded and inst_ts > inst_mtime:
            endpoint = f"{self.base_url}/api/kara/{kid}/download/inst"
            await self.download_url(endpoint, inst_filename, inst_ts)

    async def download_font(self, font: Font) -> None:
        font_filename = self.fonts_base_dir / f"{font.ID}.ttf"
        if font_filename.exists():
            return

        endpoint = f"{self.base_url}/api/font/{font.ID}/download"
        await self.download_url(endpoint, font_filename, None)


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


async def sync(base_url: str, token: str, dest_dir: Path, parallel: int = 4):
    headers = {"Authorization": f"Bearer {token}", "Range": "bytes=0-"}
    timeout = httpx.Timeout(connect=10, read=600, write=300, pool=10)
    async with httpx.AsyncClient(headers=headers, timeout=timeout) as hclient:
        client = KaraberusClient(hclient, base_url, dest_dir)
        fonts = await client.get_fonts()
        karas = await client.get_karas()

        font_coros = (client.download_font(font) for font in fonts)
        kara_coros = (client.download(kara) for kara in karas)

        downloads = DownloadRunner(itertools.chain(font_coros, kara_coros))
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
