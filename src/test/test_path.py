from ncbiloader.loader import NCBILoader


def test_loader_creates_directories(tmp_path: str) -> None:
    """Проверяем, что лоадер правильно создает структуру папок"""

    # tmp_path - это объект Path временной папки (напр. /tmp/pytest_1/)
    loader = NCBILoader(output_dir=str(tmp_path), silent=True)

    assert loader.storage.out_dir.exists()
    assert loader.storage.state_dir.exists()
    assert loader.storage.state_dir.name == ".states"
