from ncbiloader.models import Chunk, File


def test_file_chunk_generation() -> None:
    """Проверяем, что File правильно режет себя на чанки"""

    # 1. Arrange (Подготовка)
    total_size = 105
    chunk_size = 50

    # 2. Act (Действие)
    file_obj = File(
        filename="test.txt", url="http://fake.url", content_length=total_size, chunk_size=chunk_size
    )

    # 3. Assert (Проверка)
    assert len(file_obj.chunks) == 3  # 50 + 50 + 5

    # Проверяем границы первого куска
    assert file_obj.chunks[0].start == 0
    assert file_obj.chunks[0].end == 49

    # Проверяем последний кусок (он должен быть обрезан)
    assert file_obj.chunks[-1].start == 100
    assert file_obj.chunks[-1].end == 104


def test_chunk_is_finished() -> None:
    """Проверяем логику завершения куска"""
    chunk = Chunk(filename="test", start=0, end=100, current_pos=0)
    assert chunk.is_finished is False

    chunk.current_pos = 101
    assert chunk.is_finished is True


def test_to_json_in_json() -> None:

    total_size = 105
    chunk_size = 50

    file_obj = File(
        filename="test.txt", url="http://fake.url", content_length=total_size, chunk_size=chunk_size
    )

    test_obj = File.from_json(file_obj.to_json())

    assert test_obj.chunk_size == file_obj.chunk_size
    assert test_obj.content_length == file_obj.content_length
    assert test_obj.filename == file_obj.filename
    assert test_obj.url == file_obj.url
