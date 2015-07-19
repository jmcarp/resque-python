import json

import mock
import pytest

from resque_python import resque

class Bunch:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

@pytest.fixture
def mock_redis():
    return mock.Mock()

@pytest.fixture
def q(mock_redis):
    return resque.Resque(mock_redis)

@pytest.fixture
def callbacks(q, monkeypatch):
    mock_success, mock_error = mock.Mock(), mock.Mock()
    monkeypatch.setattr(q, '_success', mock_success)
    monkeypatch.setattr(q, '_error', mock_error)
    return Bunch(success=mock_success, error=mock_error)

class TestResque:

    def test_perform(self, q, callbacks):
        @q.task('queue')
        def succeed(*args):
            pass
        body = {
            'class': 'succeed',
            'args': [1, 2, 3],
        }
        q.perform('queue', json.dumps(body))
        callbacks.success.assert_called_with('queue', body)
        assert not callbacks.error.called

    def test_perform_missing_job(self, q, callbacks):
        body = {
            'class': 'missing',
            'args': [1, 2, 3],
        }
        q.perform('queue', json.dumps(body))
        assert callbacks.error.called
        call = callbacks.error.call_args[0]
        assert call[:2] == ('queue', body)
        assert isinstance(call[2], resque.MissingJobError)
        assert call[2].args == ('No job named "missing"', )
        assert not callbacks.success.called

    def test_perform_error(self, q, callbacks):
        @q.task('queue', 'fail')
        def fail(*args):
            raise RuntimeError('This always fails')
        body = {
            'class': 'fail',
            'args': [1, 2, 3],
        }
        q.perform('queue', json.dumps(body))
        assert callbacks.error.called
        call = callbacks.error.call_args[0]
        assert call[:2] == ('queue', body)
        assert isinstance(call[2], RuntimeError)
        assert call[2].args == ('This always fails', )
        assert not callbacks.success.called
