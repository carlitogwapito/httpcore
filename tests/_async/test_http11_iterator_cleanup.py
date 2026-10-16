import typing

import anyio
import httpcore
import pytest


class BodyWithoutClose:
    def __init__(self) -> None:
        self.chunks = iter((b"one", b"two", b"three"))

    def __aiter__(self) -> "BodyWithoutClose":
        return self

    async def __anext__(self) -> bytes:
        try:
            return next(self.chunks)
        except StopIteration:
            raise StopAsyncIteration from None


class BodyWithClose(BodyWithoutClose):
    def __init__(self) -> None:
        super().__init__()
        self.close_calls = 0

    async def aclose(self) -> None:
        await anyio.sleep(0)
        self.close_calls += 1


class WriteStream(httpcore.AsyncMockStream):
    def __init__(self, fail_write: bool) -> None:
        self.writes = 0
        self.fail_write = fail_write
        status = b"413 Payload Too Large" if fail_write else b"200 OK"
        super().__init__([b"HTTP/1.1 " + status + b"\r\nContent-Length: 0\r\n\r\n"])

    async def write(
        self, buffer: bytes, timeout: typing.Optional[float] = None
    ) -> None:
        self.writes += 1
        if self.fail_write and self.writes == 3:
            raise httpcore.WriteError("synthetic mid-body failure")
        await super().write(buffer, timeout)


@pytest.mark.anyio
@pytest.mark.parametrize("closable", [False, True], ids=["no-hook", "close-hook"])
@pytest.mark.parametrize("fail_write", [False, True], ids=["success", "write-error"])
async def test_ordinary_request_iterator_cleanup(closable, fail_write):
    body = BodyWithClose() if closable else BodyWithoutClose()
    stream = WriteStream(fail_write)
    origin = httpcore.Origin(b"https", b"example.com", 443)

    async with httpcore.AsyncHTTP11Connection(origin=origin, stream=stream) as conn:
        response = await conn.request("POST", "https://example.com/", content=body)
        assert response.status == (413 if fail_write else 200)
        assert response.content == b""

    if fail_write:
        assert stream.writes == 3
    else:
        assert stream.writes >= 4
    if closable:
        assert body.close_calls == 1
