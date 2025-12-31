import asyncio
import json
import logging
import os
import signal
import time
import traceback
import types
from contextlib import asynccontextmanager
from http import HTTPStatus
from typing import Any, Dict, List, Optional, Tuple, Union

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from gunicorn.app.base import Arbiter, BaseApplication
from starlette.applications import Starlette
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR
from starlette.types import ASGIApp
from uvicorn.workers import UvicornWorker

from ..abstractions.base.runner import (
    ASGI_SERVE_STUB_TYPE,
    ASGI_STUB_TYPE,
    ENDPOINT_SERVE_STUB_TYPE,
)
from ..channel import runner_context
from ..clients.gateway import (
    EndTaskRequest,
    GatewayServiceStub,
    StartTaskRequest,
)
from ..middleware import (
    TaskLifecycleData,
    TaskLifecycleMiddleware,
    WebsocketTaskLifecycleMiddleware,
)
from ..runner.common import (
    FunctionContext,
    FunctionHandler,
    execute_lifecycle_method_async,
    wait_for_checkpoint,
)
from ..runner.common import config as cfg
from ..type import LifeCycleMethod, TaskStatus
from .common import end_task_and_send_callback, is_asgi3


class EndpointFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if record.args:
            return record.args and len(record.args) >= 3 and record.args[2] != "/health"
        return True


class GunicornArbiter(Arbiter):
    def init_signals(self):
        super(GunicornArbiter, self).init_signals()

        # Add a custom signal handler to kill gunicorn master process with non-zero exit code.
        signal.signal(signal.SIGUSR1, self.handle_usr1)
        signal.siginterrupt(signal.SIGUSR1, True)

    # Override default usr1 handler to force shutdown server when forked processes crash
    # during startup
    def handle_usr1(self, sig, frame):
        os._exit(1)


class GunicornApplication(BaseApplication):
    def __init__(self, app: ASGIApp, options: Optional[Dict] = None) -> None:
        self.options = options or {}
        self.application = app
        super().__init__()

    def load_config(self) -> None:
        for key, value in self.options.items():
            if value is not None:
                self.cfg.set(key.lower(), value)

    def load(self) -> ASGIApp:
        return Starlette()  # Return a base Starlette app -- which will be replaced post-fork

    def run(self):
        GunicornArbiter(self).run()

    @staticmethod
    def post_fork_initialize(_, worker: UvicornWorker):
        logger = logging.getLogger("uvicorn.access")
        logger.addFilter(EndpointFilter())

        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(handler)
        logger.propagate = False

        try:
            mg = EndpointManager(logger=logger, worker=worker)
            asgi_app: ASGIApp = mg.app

            # Override the default starlette app
            worker.app.callable = asgi_app

            # If checkpointing is enabled, wait for all workers to be ready before creating a checkpoint
            if cfg.checkpoint_enabled:
                wait_for_checkpoint()

        except EOFError:
            return
        except BaseException:
            logger.exception("Exiting worker due to startup error")
            if cfg.stub_type in [ENDPOINT_SERVE_STUB_TYPE, ASGI_SERVE_STUB_TYPE]:
                return

            # We send SIGUSR1 to indicate to the gunicorn master that the server should shut down completely
            # since our asgi_app callable is erroring out.
            os.kill(os.getppid(), signal.SIGUSR1)
            os._exit(1)


class OnStartMethodHandler:
    def __init__(self, worker: UvicornWorker) -> None:
        self._is_running = True
        self._worker = worker

    async def start(self):
        loop = asyncio.get_running_loop()
        task = loop.create_task(self._keep_worker_alive())
        result = await execute_lifecycle_method_async(LifeCycleMethod.OnStart)
        self._is_running = False
        await task
        return result

    async def _keep_worker_alive(self) -> None:
        while self._is_running:
            self._worker.notify()
            await asyncio.sleep(1)


def get_task_lifecycle_data(request: Request):
    return request.state.task_lifecycle_data


class EndpointManager:
    @asynccontextmanager
    async def lifespan(self, _: FastAPI):
        with runner_context() as channel:
            self.app.state.gateway_stub = GatewayServiceStub(channel)
            yield

    def __init__(self, logger: logging.Logger, worker: UvicornWorker) -> None:
        self.logger = logger
        self.pid: int = os.getpid()
        self.exit_code: int = 0

        self.handler: FunctionHandler = FunctionHandler()
        self.on_start_value = asyncio.run(OnStartMethodHandler(worker).start())

        self.is_asgi_stub = ASGI_STUB_TYPE in cfg.stub_type
        if self.is_asgi_stub:
            context = FunctionContext.new(
                config=cfg,
                task_id=None,
                on_start_value=self.on_start_value,
            )

            app: Union[FastAPI, None] = None
            internal_asgi_app = getattr(self.handler.handler.func, "internal_asgi_app", None)
            if internal_asgi_app is not None:
                app = internal_asgi_app
                app.context = context
                app.handler = self.handler
            else:
                app = self.handler(context)

            self.app = app
            if not is_asgi3(self.app):
                raise ValueError("Invalid ASGI app returned from handler")

            self.app.router.lifespan_context = self.lifespan
        else:
            self.app = FastAPI(lifespan=self.lifespan)

        self.app.add_middleware(TaskLifecycleMiddleware)
        self.app.add_middleware(WebsocketTaskLifecycleMiddleware)

        # Register signal handlers
        signal.signal(signal.SIGTERM, self.shutdown)

        if hasattr(self.app, "get"):

            @self.app.get("/health")
            async def health():
                return Response(status_code=HTTPStatus.OK)
        else:
            from starlette.responses import Response as StarletteResponse
            from starlette.routing import Route

            async def health(request):
                return StarletteResponse(status_code=HTTPStatus.OK)

            self.app.router.routes.append(Route("/health", health))

        if self.is_asgi_stub:
            return

        @self.app.get("/")
        @self.app.post("/")
        async def function(
            request: Request,
            task_lifecycle_data: TaskLifecycleData = Depends(get_task_lifecycle_data),
        ):
            task_id = request.headers.get("X-TASK-ID")
            payload = await request.json()

            status_code = HTTPStatus.OK
            task_lifecycle_data.result, error = await self._call_function(
                task_id=task_id, payload=payload
            )

            if error:
                task_lifecycle_data.status = TaskStatus.Error
                status_code = HTTPStatus.INTERNAL_SERVER_ERROR

            kwargs = payload.get("kwargs", {})
            if kwargs:
                task_lifecycle_data.override_callback_url = kwargs.get("callback_url")

            # Handle streaming responses
            if hasattr(task_lifecycle_data.result, "__aiter__"):
                # Async generator - return streaming response
                async def generate():
                    async for item in task_lifecycle_data.result:
                        if isinstance(item, dict):
                            yield f"data: {json.dumps(item)}\n\n"
                        else:
                            yield f"data: {json.dumps({'data': item})}\n\n"

                return StreamingResponse(
                    generate(),
                    media_type="text/plain",
                    headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
                )
            elif isinstance(task_lifecycle_data.result, types.GeneratorType):
                # Regular generator - return streaming response
                def generate():
                    for item in task_lifecycle_data.result:
                        if isinstance(item, dict):
                            yield f"data: {json.dumps(item)}\n\n"
                        else:
                            yield f"data: {json.dumps({'data': item})}\n\n"

                return StreamingResponse(
                    generate(),
                    media_type="text/plain",
                    headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
                )

            return self._create_response(body=task_lifecycle_data.result, status_code=status_code)

        @self.app.post("/__batch__")
        async def batch_function(request: Request):
            """
            Accepts user's batched inputs via single JSON (1 JSON per batch),
            Starts tasks for each item and calls user's batch function,
            Ends tasks' lifecycles and returns outputs

            Sample input JSON:
            {
              "batch_id": "<batch-id>",
              "items": [
                {
                  "task_id": "<task-id-0>",
                  "payload": {
                    "args": ["hello"],
                    "kwargs": {"x": 123}
                  }
                },
                {
                  "task_id": "<task-id-1>",
                  "payload": {
                    "args": ["world"],
                    "kwargs": {"x": 456}
                  }
                }
              ]
            }

            # ! Batched tasks skip the middleware. All middleware logic for batched tasks is handled here.
            """
            gateway_stub = request.app.state.gateway_stub

            # CRITICAL FIX #7: Validate batch payload structure
            try:
                batch_payload = await request.json()
            except Exception as e:
                return JSONResponse(
                    {"error": f"Invalid JSON payload: {str(e)}"},
                    status_code=HTTPStatus.BAD_REQUEST,
                )

            # Validate required fields
            if not isinstance(batch_payload, dict):
                return JSONResponse(
                    {"error": "Batch payload must be a JSON object"},
                    status_code=HTTPStatus.BAD_REQUEST,
                )

            batch_id = batch_payload.get("batch_id")
            if not batch_id or not isinstance(batch_id, str):
                return JSONResponse(
                    {"error": "Missing or invalid 'batch_id' field"},
                    status_code=HTTPStatus.BAD_REQUEST,
                )

            items = batch_payload.get("items")
            if not items or not isinstance(items, list):
                return JSONResponse(
                    {"error": "Missing or invalid 'items' field - must be a non-empty list"},
                    status_code=HTTPStatus.BAD_REQUEST,
                )

            # Validate each item has required fields
            for idx, item in enumerate(items):
                if not isinstance(item, dict):
                    return JSONResponse(
                        {"error": f"Item at index {idx} must be a JSON object"},
                        status_code=HTTPStatus.BAD_REQUEST,
                    )
                if "task_id" not in item:
                    return JSONResponse(
                        {"error": f"Item at index {idx} missing 'task_id' field"},
                        status_code=HTTPStatus.BAD_REQUEST,
                    )
                if "payload" not in item:
                    return JSONResponse(
                        {"error": f"Item at index {idx} missing 'payload' field"},
                        status_code=HTTPStatus.BAD_REQUEST,
                    )

            # Extract batch_index for proper demuxing (Go may skip failed items)
            batch_indices = [item.get("batch_index", i) for i, item in enumerate(items)]
            payloads = [item["payload"] for item in items]
            task_ids = [item["task_id"] for item in items]

            for task_id in task_ids:
                gateway_stub.start_task(StartTaskRequest(
                    task_id=task_id,
                    container_id=cfg.container_id
                ))

            start_time = time.time()

            results, errors = await self._call_batch_function(batch_id, task_ids, payloads)
            total_duration = time.time() - start_time
            # HIGH FIX #18: Calculate per-task duration as total / batch_size
            # This provides a more accurate per-task duration for billing/metrics
            per_task_duration = total_duration / len(task_ids) if task_ids else 0

            for idx, task_id in enumerate(task_ids):
                status = TaskStatus.Error if errors[idx] else TaskStatus.Complete
                end_task_and_send_callback(
                    gateway_stub=gateway_stub,
                    payload=results[idx],
                    end_task_request=EndTaskRequest(
                        task_id=task_id,
                        container_id=cfg.container_id,
                        keep_warm_seconds=cfg.keep_warm_seconds,
                        task_status=status,
                        task_duration=per_task_duration,
                    ),
                    override_callback_url=None,
                )

            return JSONResponse(
                {
                    "batch_id": batch_id,
                    "results": [
                        {
                            "batch_index": batch_indices[i],
                            "status_code": HTTP_500_INTERNAL_SERVER_ERROR if errors[i] else 200,
                            "body": results[i]
                        }
                        for i in range(len(items))
                    ]
                }
            )


    def _create_response(self, *, body: Any, status_code: int = HTTPStatus.OK) -> Response:
        if isinstance(body, Response):
            return body

        try:
            return JSONResponse(body, status_code=status_code)
        except BaseException:
            self.logger.exception("Response serialization failed")
            return JSONResponse(
                {"error": traceback.format_exc()},
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    async def _call_function(self, task_id: str, payload: dict) -> Tuple[Response, Any]:
        response_body = {}
        error = None

        args = payload.get("args", [])
        if args is None:
            args = []

        kwargs = payload.get("kwargs", {})
        if kwargs is None:
            kwargs = {}

        context = FunctionContext.new(
            config=cfg,
            task_id=task_id,
            on_start_value=self.on_start_value,
        )

        try:
            if self.handler.is_async:
                response_body = await self.handler.__acall__(
                    context,
                    *args,
                    **kwargs,
                )
            else:
                response_body = self.handler(
                    context,
                    *args,
                    **kwargs,
                )
        except BaseException:
            exception = traceback.format_exc()
            print(exception)
            response_body = {"error": exception}
            error = exception

        return response_body, error

    async def _call_batch_function(self, batch_id: str, task_ids: List[str], payloads: List[dict]) -> Tuple[List[Any], List[Any]]:
        """
        Accepts batched inputs, builds batch context via FunctionContext.new() and inputs the batched inputs into the users function.
        IMP: User's function must accept list inputs - else will error

        Error: If user's function returns outputs > batch size, will error
        """
        results = []
        errors = []

        args = [p.get("args", []) or [] for p in payloads]
        kwargs = [p.get("kwargs", {}) or {} for p in payloads]

        context = FunctionContext.new(
            config=cfg,
            task_id=batch_id,
            on_start_value=self.on_start_value,
        )

        try:
            if self.handler.is_async:
                batch_results = await self.handler.__acall__(context, args, kwargs)
            else:
                batch_results = self.handler(context, args, kwargs)
            
            if isinstance(batch_results, list) and len(batch_results) == len(payloads):
                results = batch_results
                errors = [None] * len(payloads)
            else:
                # User function returned wrong format - error out
                err_msg = f"Batch function must return list of {len(payloads)} results, got {type(batch_results).__name__}"
                print(err_msg)
                results = [{"error": err_msg}] * len(payloads)
                errors = [err_msg] * len(payloads)
        
        except BaseException:
            exception = traceback.format_exc()
            print(exception)
            results = [{"error": exception}]*len(payloads)
            errors = [exception]*len(payloads)
        
        return results, errors

    def shutdown(self, signum=None, frame=None):
        os._exit(self.exit_code)


if __name__ == "__main__":
    options = {
        "bind": [f"[::]:{cfg.bind_port}"],
        "workers": cfg.workers,
        "worker_class": "uvicorn.workers.UvicornWorker",
        "loglevel": "info",
        "post_fork": GunicornApplication.post_fork_initialize,
        "timeout": cfg.timeout,
    }

    if os.environ.get("GUNICORN_NO_WAIT") == "true":
        options["graceful_timeout"] = 0

    GunicornApplication(Starlette(), options).run()
