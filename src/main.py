import asyncio
import os
import zlib

import typer

from ncbiloader import NCBILoader


async def save_stream_to_disk(loader, url, output_dir="."):
    print(url)
    # 1. Придумываем имя (отрезаем .gz)
    filename = url[0].rsplit("/", 1)[1]
    if filename.endswith(".gz"):
        out_name = filename[:-3]
    else:
        out_name = filename + ".unpacked"

    out_path = os.path.join(output_dir, out_name)
    print(f"[*] Скачиваем и распаковываем в: {out_path}")

    # 2. Создаем декомпрессор
    d = zlib.decompressobj(zlib.MAX_WBITS | 16)

    # 3. Открываем файл на запись
    with open(out_path, "wb") as f:
        # Получаем поток от твоего лоадера
        async for chunk in loader.stream(url):
            # Декомпрессия
            data = d.decompress(chunk)
            if data:
                f.write(data)

        # Дописываем хвосты
        f.write(d.flush())


async def main(
    links: str | list[str],
    threads: int = 3,
    silent: bool = False,
    timeout: int | float = 30.0,
    follow_redirects: bool = True,
    http2: bool = False,
    verify: bool = False,
) -> None:
    async with NCBILoader(
        threads=threads,
        silent=silent,
        timeout=timeout,
        follow_redirects=follow_redirects,
        http2=http2,
        verify=verify,
    ) as loader:
        await save_stream_to_disk(loader, links)
        # await loader.run(links)

    # links = [
    #     "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/000/001/405/GCA_000001405.15_GRCh38/seqs_for_alignment_pipelines.ucsc_ids/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.gz",
    #     "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14/GCF_000001405.40_GRCh38.p14_rm.out.gz",
    #     "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14/GCF_000001405.40_GRCh38.p14_translated_cds.faa.gz",
    #     "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14/GCF_000001405.40_GRCh38.p14_cds_from_genomic.fna.gz",
    #     "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14/GCF_000001405.40_GRCh38.p14_assembly_stats.txt",
    # ]

    # 3. Запускаем загрузку ВНУТРИ контекста монитора


def ncbiloader(
    links: list[str] = typer.Argument(..., help="Список URL для скачивания"),
    threads: int = typer.Option(3, "--threads", "-t", help="Количество потоков"),
    silent: bool = typer.Option(False, "--silent", "-s", help="Без графики"),
    timeout: int = 30,
    follow_redirects: bool = True,
    http2: bool = True,
    verify: bool = False,
) -> None:

    if not links:
        typer.secho("Нет ссылок для скачивания", color=typer.colors.RED, bold=True)
        return
    try:
        asyncio.run(
            main(
                links=links,
                threads=threads,
                silent=silent,
                timeout=timeout,
                follow_redirects=follow_redirects,
                http2=http2,
                verify=verify,
            )
        )
    except KeyboardInterrupt:
        typer.echo("\nПрервано пользователем (CLI).", err=True)


if __name__ == "__main__":
    typer.run(ncbiloader)
