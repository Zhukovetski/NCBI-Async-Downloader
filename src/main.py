# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import os
import zlib

import typer

from ncbiloader import NCBILoader


async def save_stream_to_disk(loader, url, output_dir="."):
    filename = url[0].rsplit("/", 1)[1]
    if filename.endswith(".gz"):
        out_name = filename[:-3]
    else:
        out_name = filename + ".unpacked"

    out_path = os.path.join(output_dir, out_name)
    print(f"[*] Скачиваем и распаковываем в: {out_path}")

    d = zlib.decompressobj(zlib.MAX_WBITS | 16)

    with open(out_path, "wb") as f:
        async for chunk in loader.stream(url):
            data = d.decompress(chunk)
            if data:
                f.write(data)

        f.write(d.flush())


def file_url_generator(filepath: str):
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):  # Игнор пустых и комментов
                yield line


async def main(
    links: str | list[str],
    stream: bool,
    threads: int = 3,
    silent: bool = False,
    timeout: int | float = 30.0,
    follow_redirects: bool = True,
    stream_buffer_size: int = 500,
    http2: bool = False,
    verify: bool = False,
) -> None:

    async with NCBILoader(
        threads=threads,
        silent=silent,
        timeout=timeout,
        follow_redirects=follow_redirects,
        stream_buffer_size=stream_buffer_size,
        http2=http2,
        verify=verify,
    ) as loader:
        if stream:
            await loader.stream(links)
        else:
            await loader.run(links)


def loader(
    links: list[str] = typer.Argument(..., help="Список URL для скачивания"),
    stream: bool = typer.Option(False, "--stream, -s", help="Вывод потоком"),
    threads: int = typer.Option(3, "--threads", "-t", help="Количество потоков"),
    silent: bool = typer.Option(False, "--silent", "-sl", help="Без графики"),
    timeout: int = 30,
    follow_redirects: bool = typer.Option(
        True, "--follow_redirects/--no-follow_redirects", "-fr/-nfr", help=""
    ),
    stream_buffer_size: int = typer.Option(
        500, "--stream_buffer_size", "-sbs", help="Максимальный размер буфера"
    ),
    http2: bool = typer.Option(
        True, "--http2/--no-http2", "-h2/-nh2", help="Включить поддержку HTTP/2"
    ),
    verify: bool = False,
) -> None:

    if not links:
        typer.secho("Нет ссылок для скачивания", color=typer.colors.RED, bold=True)
        return
    try:
        asyncio.run(
            main(
                links=links,
                stream=stream,
                threads=threads,
                silent=silent,
                timeout=timeout,
                follow_redirects=follow_redirects,
                stream_buffer_size=stream_buffer_size,
                http2=http2,
                verify=verify,
            )
        )
    except KeyboardInterrupt:
        typer.echo("\nПрервано пользователем (CLI).", err=True)


if __name__ == "__main__":
    typer.run(loader)
