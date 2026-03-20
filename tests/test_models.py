from hydrastream.models import Chunk, File, FileMeta


def test_chunk_properties() -> None:
    chunk = Chunk(filename="test.gz", start=100, end=199, current_pos=150)

    assert chunk.size == 100
    assert chunk.uploaded == 51
    assert chunk.remaining == 50
    assert chunk.is_finished is False

    chunk.current_pos = 200
    assert chunk.is_finished is True


def test_file_chunk_generation() -> None:
    meta = FileMeta(
        filename="genome.fna",
        url="http://example.com/genome.fna",
        content_length=105,
    )
    file_obj = File(
        meta=meta,
        chunk_size=50,
    )

    assert len(file_obj.chunks) == 3

    assert file_obj.chunks[0].start == 0
    assert file_obj.chunks[0].end == 49

    assert file_obj.chunks[1].start == 50
    assert file_obj.chunks[1].end == 99

    assert file_obj.chunks[2].start == 100
    assert file_obj.chunks[2].end == 104


def test_chunk_is_finished() -> None:
    chunk = Chunk(filename="test", start=0, end=100, current_pos=0)
    assert chunk.is_finished is False

    chunk.current_pos = 101
    assert chunk.is_finished is True


def test_to_json_in_json() -> None:
    total_size = 105
    chunk_size = 50

    meta = FileMeta(
        filename="test.txt",
        url="http://fake.url",
        content_length=total_size,
    )
    file_obj = File(
        meta=meta,
        chunk_size=chunk_size,
    )

    test_obj = File.from_json(file_obj.to_json())

    assert test_obj.chunk_size == file_obj.chunk_size
    assert test_obj.meta.content_length == file_obj.meta.content_length
    assert test_obj.meta.filename == file_obj.meta.filename
    assert test_obj.meta.url == file_obj.meta.url
