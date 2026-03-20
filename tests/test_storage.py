import asyncio
import hashlib
import os
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pytest_mock import MockerFixture

from hydrastream.engine import run_downloads
from hydrastream.models import (
    Chunk,
    File,
    FileMeta,
    HydraConfig,
    HydraContext,
    StorageState,
    UIState,
)
from hydrastream.storage import (
    autosave,
    create_sparse_file,
    delete_state,
    get_state_path,
    get_unique_path,
    load_state,
    open_file,
    save_all_states,
    save_state,
    verify_file_hash,
    verify_size,
    verify_stream,
    write_chunk_data,
)


async def test_loader_creates_directories(tmp_path: str) -> None:

    config = HydraConfig(out_dir=str(tmp_path), quiet=True)
    state = HydraContext(config=config)
    await run_downloads(state, "")

    assert state
    assert state.fs.out_dir.exists()
    assert state.fs.state_dir.exists()
    assert state.fs.state_dir.name == ".states"


def test_storage_creates_directories(tmp_path: Path) -> None:

    storage = StorageState(out_dir=tmp_path, ui=UIState(log_file=tmp_path / "log"))

    assert storage.out_dir.exists()
    assert storage.state_dir.exists()
    assert storage.state_dir.name == ".states"


@pytest.mark.asyncio
async def test_write_chunk_data_out_of_order(tmp_path: Path) -> None:

    storage = StorageState(out_dir=tmp_path, ui=UIState(log_file=tmp_path / "log"))
    filename = "genome_test.bin"
    file_size = 10

    create_sparse_file(storage, filename, file_size)

    fd = open_file(storage, filename)

    try:
        await write_chunk_data(fd, bytearray(b"B"), 5)
        await write_chunk_data(fd, bytearray(b"A"), 0)
        await write_chunk_data(fd, bytearray(b"C"), 9)
    finally:
        os.close(fd)

    final_file = tmp_path / filename
    content = final_file.read_bytes()

    assert len(content) == 10
    assert content[0:1] == b"A"
    assert content[5:6] == b"B"
    assert content[9:10] == b"C"

    assert content[1:5] == b"\x00\x00\x00\x00"


def test_save_and_load_state(tmp_path: Path) -> None:

    storage = StorageState(out_dir=tmp_path, ui=UIState(log_file=tmp_path / "log"))

    meta = FileMeta(
        filename="data.tar",
        url="http://",
        content_length=1000,
    )
    file_obj = File(meta=meta, chunk_size=500)

    (tmp_path / "data.tar").touch()

    save_state(storage, file_obj)

    assert (tmp_path / ".states" / "data.tar.state.json").exists()

    loaded_file, count = load_state(storage, "data.tar")

    assert loaded_file is not None
    assert count == 1
    assert loaded_file.meta.filename == "data.tar"
    assert len(loaded_file.chunks) == 2


@pytest.mark.asyncio
async def test_verify_file_hash(tmp_path: Path) -> None:
    storage = StorageState(out_dir=tmp_path, ui=UIState(log_file=tmp_path / "log"))

    real_content = b"The quick brown fox jumps over the lazy dog"
    real_md5 = hashlib.md5(real_content).hexdigest()

    (tmp_path / "fox.txt").write_bytes(real_content)

    meta = FileMeta(
        filename="fox.txt",
        url="",
        content_length=len(real_content),
        expected_md5=real_md5,
    )
    good_file = File(meta=meta, chunk_size=10)
    for chunk in good_file.chunks:
        chunk.current_pos = chunk.end + 1
    await verify_file_hash(storage, good_file)

    meta = FileMeta(
        filename="fox.txt",
        url="",
        content_length=len(real_content),
        expected_md5="1234567890abcdef",
    )
    bad_file = File(meta=meta, chunk_size=10)
    for chunk in bad_file.chunks:
        chunk.current_pos = chunk.end + 1
    assert bad_file.is_complete is True
    with pytest.raises(ValueError, match=r"Hash mismatch for fox.txt"):
        await verify_file_hash(storage, bad_file)


# --- ФИКСТУРЫ ---


@pytest.fixture
def storage_state(tmp_path: Path) -> StorageState:
    """Создает StorageState с временной директорией"""
    state_dir = tmp_path / ".states"
    state_dir.mkdir(parents=True, exist_ok=True)
    ui = MagicMock(spec=UIState)
    return StorageState(ui=ui, out_dir=tmp_path)


@pytest.fixture
def dummy_file() -> File:
    """Создает базовый объект File для тестов"""
    meta = FileMeta(
        filename="test.bin",
        url="http://test.com",
        content_length=100,
        expected_md5=None,
    )
    chunks = [
        Chunk(filename="test.bin", start=0, end=49, current_pos=0),
        Chunk(filename="test.bin", start=50, end=99, current_pos=50),
    ]
    return File(meta=meta, chunk_size=50, chunks=chunks)


@pytest.fixture
def mock_ctx(storage_state: StorageState) -> MagicMock:
    """Фейковый HydraContext для тестов, требующих ctx"""
    ctx = MagicMock(spec=HydraContext)
    ctx.is_running = True
    ctx.fs = storage_state
    ctx.ui = MagicMock(spec=UIState)
    return ctx


# --- ТЕСТЫ БАЗОВЫХ УТИЛИТ (Строки 19-35, 50, 56) ---


def test_get_unique_path(tmp_path: Path) -> None:
    """Проверяет генерацию уникальных имен файлов при коллизиях"""
    file_path = tmp_path / "data.txt"

    # 1. Файла нет -> возвращает тот же путь
    assert get_unique_path(file_path) == file_path

    # 2. Файл есть -> возвращает " (1)"
    file_path.touch()
    new_path = get_unique_path(file_path)
    assert new_path.name == "data (1).txt"

    # 3. Файл " (1)" тоже есть -> возвращает " (2)"
    new_path.touch()
    newer_path = get_unique_path(file_path)
    assert newer_path.name == "data (2).txt"


def test_create_sparse_file(storage_state: StorageState) -> None:
    """Строки 38-57: Проверка создания пустого файла нужного размера"""
    filename = "sparse.bin"
    size = 1024

    # Нормальное создание
    res = create_sparse_file(storage_state, filename, size)
    assert res is None  # Имя не изменилось
    assert (storage_state.out_dir / filename).stat().st_size == size

    # Коллизия имен
    res_collision = create_sparse_file(storage_state, filename, size)
    assert res_collision == "sparse (1).bin"

    # Ошибка нехватки места
    with patch("shutil.disk_usage") as mock_usage:
        mock_usage.return_value.free = 500  # Места меньше, чем надо (1024)
        with pytest.raises(OSError, match="Insufficient disk space"):
            create_sparse_file(storage_state, "fail.bin", size)


# --- ТЕСТЫ I/O (Строки 60-70) ---


@pytest.mark.asyncio
async def test_open_and_write_chunk(storage_state: StorageState) -> None:
    """Проверяет открытие дескриптора и асинхронную запись (os.pwrite)"""
    filename = "write_test.bin"
    create_sparse_file(storage_state, filename, 10)

    fd = open_file(storage_state, filename)
    assert fd > 0

    try:
        # Пишем в середину
        await write_chunk_data(fd, bytearray(b"HELLO"), offset=2)
    finally:
        os.close(fd)

    content = (storage_state.out_dir / filename).read_bytes()
    assert content[2:7] == b"HELLO"


# --- ТЕСТЫ СОСТОЯНИЯ (Строки 77-108, 123-146, 149-151) ---


def test_save_and_load_state2(storage_state: StorageState, dummy_file: File) -> None:
    """Покрывает save_state, load_state и delete_state"""

    # ВАЖНО: load_state проверяет наличие скачиваемого файла на диске!
    (storage_state.out_dir / "test.bin").touch()

    # 1. Сохраняем
    save_state(storage_state, dummy_file)
    state_path = get_state_path(storage_state, "test.bin")
    assert state_path.exists()

    # 2. Загружаем
    loaded_file, count = load_state(storage_state, "test.bin")
    assert loaded_file is not None
    assert loaded_file.meta.filename == "test.bin"
    assert count == 1

    # 3. Удаляем
    delete_state(storage_state, "test.bin")
    assert not state_path.exists()


def test_load_state_errors(storage_state: StorageState) -> None:
    """Покрывает строки 128-129, 137-138 (ошибки загрузки стейта)"""
    # Файла нет
    assert load_state(storage_state, "missing.bin") == (None, 0)

    # Битая JSON структура
    bad_state = get_state_path(storage_state, "bad.bin")
    bad_state.write_text("{bad json")
    (storage_state.out_dir / "bad.bin").touch()  # Файл-пустышка нужен для проверки

    assert load_state(storage_state, "bad.bin") == (None, 1)


def test_save_all_states(
    mocker: MockerFixture, storage_state: StorageState, dummy_file: File
) -> None:
    mock_save_state: MagicMock = mocker.patch("hydrastream.storage.save_state")
    """Строки 103-108: Проверяем, что завершенные файлы НЕ сохраняются"""
    files = {"test.bin": dummy_file}

    # Пока чанки не завершены, стейт сохраняется
    save_all_states(storage_state, files)
    mock_save_state.assert_called_once()

    mock_save_state.reset_mock()

    # Завершаем все чанки
    for c in dummy_file.chunks:
        c.current_pos = c.end + 1

    save_all_states(storage_state, files)
    mock_save_state.assert_not_called()  # Не должен сохранять завершенный файл!


@pytest.mark.asyncio
async def test_autosave_loop(mocker: MockerFixture, mock_ctx: MagicMock) -> None:
    mock_save_all: MagicMock = mocker.patch("hydrastream.storage.save_all_states")
    """Покрытие строк 111-120 (Фоновый автосейв)"""
    # Запускаем автосейв с интервалом 0.01 сек
    task = asyncio.create_task(autosave(mock_ctx, 0.01))

    # Даем ему сделать 1 круг
    await asyncio.sleep(0.02)
    mock_ctx.is_running = False  # Глушим цикл
    await task

    mock_save_all.assert_called()


# --- ТЕСТЫ ВЕРИФИКАЦИИ (Строки 154-239) ---


def test_verify_size(storage_state: StorageState, dummy_file: File) -> None:
    """Строки 154-167"""
    filepath = storage_state.out_dir / "test.bin"
    filepath.write_bytes(b"A" * 100)  # Правильный размер (100 байт)

    # Не падает
    verify_size(storage_state, dummy_file)

    # Неправильный размер
    filepath.write_bytes(b"A" * 50)
    with pytest.raises(ValueError, match="Size mismatch"):
        verify_size(storage_state, dummy_file)


@pytest.mark.asyncio
async def test_verify_file_hash2(storage_state: StorageState, dummy_file: File) -> None:
    """Покрытие 170-218 (Проверка MD5 файла)"""
    filepath = storage_state.out_dir / "test.bin"
    content = b"The quick brown fox"
    real_md5 = hashlib.md5(content).hexdigest()
    filepath.write_bytes(content)

    dummy_file.meta = replace(dummy_file.meta, expected_md5=real_md5)
    # Делаем файл "завершенным", иначе проверка выйдет сразу (строка 171)
    for c in dummy_file.chunks:
        c.current_pos = c.end + 1

    # 1. Успех
    # ВАЖНО: Мы передаем пустой UIState, чтобы метод log не упал,
    # но нам нужно его замокать, чтобы тест не лез в реальный логгер
    mock_ui = MagicMock(spec=UIState)
    mock_ctx = StorageState(
        ui=mock_ui,
        out_dir=storage_state.out_dir,
    )

    res = await verify_file_hash(mock_ctx, dummy_file)
    assert res is True
    assert dummy_file.verified is True

    # 2. Провал (неверный хеш)
    dummy_file.verified = False
    dummy_file.meta = replace(dummy_file.meta, expected_md5="wronghash")

    with pytest.raises(ValueError, match="Hash mismatch"):
        await verify_file_hash(mock_ctx, dummy_file)

    # Проверяем, что битый файл удалился!
    assert not filepath.exists()


def test_verify_stream() -> None:
    """Строки 221-239"""
    content = b"Stream data"
    real_md5 = hashlib.md5(content).hexdigest()

    hasher = hashlib.md5()
    hasher.update(content)

    # 1. Успех
    verify_stream(hasher, real_md5, next_offset=11, total_size=11)

    # 2. Провал по хешу
    with pytest.raises(ValueError, match="Integrity Check Failed"):
        verify_stream(hasher, "wrong", 11, 11)

    # 3. Провал по размеру
    with pytest.raises(ValueError, match="Incomplete stream"):
        verify_stream(hasher, real_md5, next_offset=10, total_size=11)
