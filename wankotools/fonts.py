#!/usr/bin/env python3
import logging
from pathlib import Path
from typing import Annotated

import httpx
import typer

logger = logging.getLogger(__name__)


def upload_font(base_url: str, token: str, file: Path) -> None:
    headers = {"Authorization": f"Bearer {token}"}
    with file.open("rb") as f:
        logger.info(f"uploading {file}")
        resp = httpx.post(f"{base_url}/api/font", files={"file": f}, headers=headers)
        logger.debug(resp.json())
        resp.raise_for_status()


def main(
    files: list[Annotated[Path, typer.Argument(file_okay=True, dir_okay=False)]],
    base_url: Annotated[str, typer.Option(help="base url of your karaberus instance")],
    token: Annotated[str, typer.Option(help="your access token")],
) -> None:
    for file in files:
        upload_font(base_url, token, file)


def wankofonts():
    typer.run(main)


if __name__ == "__main__":
    wankofonts()
