# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
from typing import Annotated

import typer

from ncbiloader import NCBILoader

# Создаем приложение Typer
app = typer.Typer(add_completion=False)


async def async_main(
    links: list[str],
    stream: bool,
    threads: int,
    silent: bool,
    output_dir: str,
    md5: str | None,  # Принимаем один MD5 как строку
    timeout: float,
    follow_redirects: bool,
    stream_buffer_size: int,
    http2: bool,
    verify: bool,
) -> None:

    # Логика хешей для CLI:
    # Если ссылка одна и хеш передан -> создаем словарь.
    # Если ссылок много -> игнорируем (или можно кинуть ошибку).
    expected_checksums: dict[str, str] = {}
    if md5 and len(links) == 1:
        expected_checksums[links[0]] = md5
    elif md5 and len(links) > 1:
        typer.secho(
            "Предупреждение: Флаг --md5 игнорируется для нескольких ссылок.",
            fg="yellow",
        )

    async with NCBILoader(
        threads=threads,
        silent=silent,
        output_dir=output_dir,
        timeout=timeout,
        follow_redirects=follow_redirects,
        stream_buffer_size=stream_buffer_size,
        http2=http2,
        verify=verify,
    ) as loader:
        if stream:
            # !!! ВАЖНО: Мы должны итерироваться по генератору, чтобы процесс шел !!!
            async for filename, file_gen in loader.stream_all(links, expected_checksums):
                if not silent:
                    typer.secho(f"Стрим запущен: {filename}", fg="blue")

                # Потребляем поток (иначе скачивание зависнет)
                # Тут можно было бы писать в stdout или пайп, но пока просто крутим цикл
                async for chunk in file_gen:
                    pass  # Просто "съедаем" байты, чтобы работал механизм проверки хеша внутри

                if not silent:
                    typer.secho(f"Стрим завершен: {filename}", fg="green")
        else:
            # Обычный режим (на диск)
            await loader.run(links, expected_checksums)


H = {
    "L": "Список URL для скачивания",
    "M": "Ожидаемый MD5 (только для одной ссылки)",
    "O": "Папка для сохранения",
    "T": "Количество потоков",
    "S": "Режим потоковой обработки (без сохранения)",
    "SL": "Отключить GUI",
    "TM": "Таймаут соединения",
    "B": "Размер буфера стрима (байт)",
    "H2": "Использовать HTTP/2",
    "R": "Следовать редиректам",
    "V": "Проверять размер файла после скачивания",
}


@app.command()
def loader(
    links: Annotated[list[str], typer.Argument(help=H["L"])],
    # Options
    md5: Annotated[str | None, typer.Option("--md5", help=H["M"])] = None,
    output_dir: Annotated[str, typer.Option("-o", "--output", help=H["O"])] = "download",
    threads: Annotated[int, typer.Option("-t", "--threads", help=H["T"])] = 3,
    stream: Annotated[bool, typer.Option("-s", "--stream", help=H["S"])] = False,
    silent: Annotated[bool, typer.Option(help=H["SL"])] = False,
    # Технические настройки
    timeout: Annotated[float, typer.Option(help=H["TM"])] = 30.0,
    stream_buffer_size: Annotated[int, typer.Option("--buffer", help=H["B"])] = 5242880,
    http2: Annotated[bool, typer.Option("--http2/--no-http2", help=H["H2"])] = True,
    follow_redirects: Annotated[bool, typer.Option("--redirects/--no-redirects", help=H["R"])] = True,
    verify: Annotated[bool, typer.Option("--verify/--no-verify", help=H["V"])] = True,
) -> None:
    """
    NCBI Async Downloader: Быстрый загрузчик геномных данных.
    """
    if not links:
        typer.secho("Нет ссылок для скачивания!", fg="red", bold=True)
        raise typer.Exit(code=1)

    try:
        asyncio.run(
            async_main(
                links=links,
                stream=stream,
                threads=threads,
                silent=silent,
                output_dir=output_dir,
                md5=md5,
                timeout=timeout,
                follow_redirects=follow_redirects,
                stream_buffer_size=stream_buffer_size,
                http2=http2,
                verify=verify,
            )
        )
    except KeyboardInterrupt:
        typer.secho("\n⛔ Прервано пользователем.", fg="yellow")
    except Exception as e:
        typer.secho(f"\n💥 Критическая ошибка: {e}", fg="red", bold=True)
        # raise e # Раскомментируй для отладки


if __name__ == "__main__":
    app()
