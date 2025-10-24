# conftest.py
import os
import pytest
import ray

@pytest.fixture(scope="session", autouse=True)
def ray_session():
    """
    Запускает локальный Ray один раз на всю сессию тестов и корректно завершает его.
    Делает так, чтобы воркеры видели ваши тестовые модули (tests/) и src/.
    """
    if ray.is_initialized():
        ray.shutdown()

    repo_root = os.getcwd()
    tests_dir = os.path.join(repo_root, "tests")

    # В runtime_env добавляем tests/, чтобы worker мог импортировать модули вида "test_*.py"
    runtime_env = {
        "working_dir": repo_root,         # упаковываем весь репозиторий
        "py_modules": [tests_dir],        # добавляем tests/ в PYTHONPATH на воркере
    }

    ray.init(
        runtime_env=runtime_env,
        include_dashboard=False,
        log_to_driver=False,
        ignore_reinit_error=True,
        # num_cpus=2,  # можно ограничить в CI
    )
    try:
        yield
    finally:
        ray.shutdown()
