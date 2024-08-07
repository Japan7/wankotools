#!/usr/bin/env python3
import logging
from pathlib import Path
from typing import Annotated, TypedDict

import backoff
import httpcore
import httpx
import typer

logger = logging.getLogger(__name__)


upload_backoff = backoff.on_exception(
    backoff.expo,
    (httpcore.ReadTimeout, httpx.ReadTimeout),
    max_time=150,
)


@upload_backoff
def upload_font(base_url: str, token: str, file: Path) -> None:
    headers = {"Authorization": f"Bearer {token}"}
    with file.open("rb") as f:
        logger.info(f"uploading {file}")
        resp = httpx.post(f"{base_url}/api/font", files={"file": f}, headers=headers)
        logger.debug(resp.json())
        resp.raise_for_status()


class Fonts(TypedDict):
    ID: int
    Name: str


class FontResponse(TypedDict):
    Fonts: list[Fonts]


def get_uploaded_fonts(base_url: str, token: str) -> list[Fonts]:
    headers = {"Authorization": f"Bearer {token}"}
    resp = httpx.get(f"{base_url}/api/font", headers=headers)
    data: FontResponse = resp.json()
    resp.raise_for_status()
    return data["Fonts"]


def main(
    files: list[Annotated[Path, typer.Argument(file_okay=True, dir_okay=False)]],
    base_url: Annotated[str, typer.Option(help="base url of your karaberus instance")],
    token: Annotated[str, typer.Option(help="your access token")],
) -> None:
    fonts = get_uploaded_fonts(base_url, token)
    font_names = {font["Name"] for font in fonts}
    for file in files:
        if file.name in font_names:
            continue
        upload_font(base_url, token, file)


def wankofonts():
    typer.run(main)


if __name__ == "__main__":
    wankofonts()
