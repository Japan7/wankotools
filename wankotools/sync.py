#!/usr/bin/env python
import asyncio
import datetime
import logging
import os
import re
from pathlib import Path
from typing import Annotated, Literal

import aiofiles
import aiojobs
import backoff
import httpcore
import httpx
import minio
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
    def __init__(
        self,
        client: httpx.AsyncClient,
        s3_client: minio.Minio,
        base_url: str,
        base_dir: Path,
    ):
        self.base_url = base_url.rstrip("/")
        self.client = client
        self.base_dir = base_dir
        self.karas_base_dir = base_dir / "karas"
        self.fonts_base_dir = base_dir / "fonts"
        self.s3_client = s3_client
        self.init_base_dir()
        self.karas: dict[int, KaraberusKara] = {}
        self.fonts: dict[int, Font] = {}

    async def __aenter__(self):
        async with asyncio.TaskGroup() as tg:
            _ = tg.create_task(self._load_fonts())
            _ = tg.create_task(self._get_karas())

        return self

    async def __aexit__(self, *_):
        pass

    async def _load_karas(self):
        karas = await self._get_karas()
        for k in karas:
            self.karas[k.ID] = k

    async def _load_fonts(self):
        fonts = await self._get_fonts()
        for f in fonts:
            self.fonts[f.ID] = f

    def init_base_dir(self):
        self.karas_base_dir.mkdir(exist_ok=True, parents=True)
        self.fonts_base_dir.mkdir(exist_ok=True, parents=True)

        gitignore = self.base_dir / ".gitignore"
        if not gitignore.exists():
            _ = gitignore.write_text("*")

    async def _get_karas(self) -> list[KaraberusKara]:
        kara_endpoint = f"{self.base_url}/api/kara"

        resp = await self.client.get(kara_endpoint)
        _ = resp.raise_for_status()
        karas = KaraberusKarasResponse.model_validate(resp.json())
        return karas.Karas

    async def _get_fonts(self) -> list[Font]:
        font_endpoint = f"{self.base_url}/api/font"
        resp = await self.client.get(font_endpoint)
        _ = resp.raise_for_status()
        fonts = KaraberusFontsResponse.model_validate(resp.json())
        return fonts.Fonts

    async def _download(self, filename: Path, resp: httpx.Response, ts: float | None):
        _ = resp.raise_for_status()
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
                    _ = await f.write(data)

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

    async def download_file(
        self,
        ftype: Literal["video", "sub", "inst", 'font'],
        id: int,
        filename: Path,
        ts: float | None,
    ):
        filename.parent.mkdir(parents=True, exist_ok=True)
        try:
            logger.info(f'downloading {filename}')
            _ = await asyncio.to_thread(
                self.s3_client.fget_object,
                "karaberus",
                f"{ftype}/{id}",
                str(filename),
            )

            if ts is not None:
                # add 1 second just in case
                os.utime(filename, (ts + 1, ts + 1))

        except Exception:
            await asyncio.to_thread(filename.unlink)
            raise

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
            await self.download_file("video", kid, vid_filename, vid_ts)

        sub_filename = self.karas_base_dir / f"{kid}.ass"
        try:
            sub_stat = await asyncio.to_thread(sub_filename.stat)
            sub_mtime = sub_stat.st_mtime
        except FileNotFoundError:
            sub_mtime = 0

        sub_ts = kara.SubtitlesModTime.timestamp()
        if kara.SubtitlesUploaded and sub_ts > sub_mtime:
            await self.download_file("sub", kid, sub_filename, sub_ts)

        inst_filename = self.karas_base_dir / f"{kid}.mka"
        try:
            inst_stat = await asyncio.to_thread(inst_filename.stat)
            inst_mtime = inst_stat.st_mtime
        except FileNotFoundError:
            inst_mtime = 0

        inst_ts = kara.InstrumentalModTime.timestamp()
        if kara.InstrumentalUploaded and inst_ts > inst_mtime:
            await self.download_file("inst", kid, inst_filename, inst_ts)

    async def download_font(self, font: Font) -> None:
        font_filename = self.fonts_base_dir / f"{font.ID}.ttf"
        if font_filename.exists():
            return

        await self.download_file('font', font.ID, font_filename, None)


async def sync(
    base_url: str,
    token: str,
    s3_endpoint: str,
    s3_access_key: str,
    s3_secret_key: str,
    s3_secure: bool,
    s3_region: str,
    dest_dir: Path,
    parallel: int = 4,
):
    headers = {"Authorization": f"Bearer {token}", "Range": "bytes=0-"}
    timeout = httpx.Timeout(connect=10, read=600, write=300, pool=10)

    s3_client = minio.Minio(
        endpoint=s3_endpoint,
        access_key=s3_access_key,
        secret_key=s3_secret_key,
        secure=s3_secure,
        region=s3_region,
    )

    async with (
        httpx.AsyncClient(headers=headers, timeout=timeout) as hclient,
        KaraberusClient(hclient, s3_client, base_url, dest_dir) as client,
        aiojobs.Scheduler(limit=parallel) as sched
    ):
        fonts = client.fonts.copy()
        for font in fonts.values():
            _ = await sched.spawn(client.download_font(font))

        karas = client.karas.copy()
        for kara in karas.values():
            _ = await sched.spawn(client.download(kara))


def main(
    base_url: str,
    token: str,
    s3_endpoint: str,
    s3_access_key: str,
    s3_secret_key: str,
    s3_secure: Annotated[bool, typer.Option("--s3-secure", "-s")] = False,
    s3_region: str = 'garage',
    dest_dir: Annotated[
        Path | None, typer.Option("--directory", "-d", file_okay=False, dir_okay=True)
    ] = None,
    parallel: Annotated[int, typer.Option("--parallel", "-p")] = 4,
):
    if dest_dir is None:
        dest_dir = Path(".")

    asyncio.run(
        sync(
            base_url,
            token,
            s3_endpoint,
            s3_access_key,
            s3_secret_key,
            s3_secure,
            s3_region,
            dest_dir,
            parallel,
        )
    )


def wankosync():
    typer.run(main)


if __name__ == "__main__":
    wankosync()
